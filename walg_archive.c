/*-------------------------------------------------------------------------
 *
 * walg_archive.c
 *
 * This file includes an archive library implementation that is
 * sending files to wal-g daemon socket for archiving.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#include "common/int.h"
#include "miscadmin.h"
#include "postmaster/pgarch.h"
#if PG_VERSION_NUM >= 16000
#include "archive/archive_module.h"
#endif
#include "storage/copydir.h"
#include "storage/fd.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "port/pg_bswap.h"
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <netinet/in.h>
#include <sys/un.h>

PG_MODULE_MAGIC;

void		_PG_init(void);
#if PG_VERSION_NUM >= 16000
const ArchiveModuleCallbacks *_PG_archive_module_init(void);
#else
void		_PG_archive_module_init(ArchiveModuleCallbacks *cb);
#endif

static char *walg_socket=NULL;
static int fd;

static bool check_walg_socket(char **newval, void **extra, GucSource source);
static int set_connection(void);
#if PG_VERSION_NUM >= 16000
static bool walg_archive_configured(ArchiveModuleState *state);
static bool walg_archive_file(ArchiveModuleState *state, const char *file, const char *path);
#else
static bool walg_archive_configured(void);
static bool walg_archive_file(const char *file, const char *path);
#endif

/*
 * _PG_init
 *
 * Defines the module's GUC.
 */
void
_PG_init(void)
{
	DefineCustomStringVariable("walg_archive.walg_socket",
							   gettext_noop("WAL-G socket for archiving."),
							   NULL,
							   &walg_socket,
							   "",
							   PGC_SIGHUP,
							   0,
							   check_walg_socket, NULL, NULL);

	MarkGUCPrefixReserved("walg_archive");
}

/*
 * _PG_archive_module_init
 *
 * Returns the module's archiving callbacks.
 */
#if PG_VERSION_NUM >= 16000

static ArchiveModuleCallbacks walg_archive = {
	.check_configured_cb = walg_archive_configured,
	.archive_file_cb = walg_archive_file
};

const ArchiveModuleCallbacks *
_PG_archive_module_init(void)
{
	return &walg_archive;
}
#else
void
_PG_archive_module_init(ArchiveModuleCallbacks *cb)
{
	AssertVariableIsOfType(&_PG_archive_module_init, ArchiveModuleInit);
	
	cb->check_configured_cb = walg_archive_configured;
	cb->archive_file_cb = walg_archive_file;
}
#endif

/*
 * check_walg_socket
 *
 * Checks that the provided file descriptor exists.
 */
static bool
check_walg_socket(char **newval, void **extra, GucSource source)
{	
	struct stat st;

	/*
	 * The default value is an empty string, we have to accept that value on this step.
	 */
	if (*newval == NULL || *newval[0] == '\0'){
		return true;
	}
		
	/*
	 * Make sure the file paths won't be too long. The docs indicate that the
	 * file names to be archived can be up to 64 characters long.
	 */
	if (strlen(*newval) + 64 + 2 >= MAXPGPATH)
	{
		GUC_check_errdetail("Path to file descriptor is too long.");
		return false;
	}

	/*
	 * Check that the specified file exists.
	 */
	if (stat(*newval, &st) != 0)
	{
		GUC_check_errdetail("Specified file does not exist.");
		return false;
	}

	return true;
}

/*
 * We use frontend/backend protocol to communicate through UNIX-socket
 * So we define message format as array of bytes:
 * 1 byte - type of message, char
 * 2 byte - (N) len of message body including first 3 bytes, uint16
 * (N - 3) byte - message body, char.
 */


/*
 * walg_archive_configured
 *
 * We check connection with wal-g socket
 * with sending test message.
 */
static bool
#if PG_VERSION_NUM >= 16000
walg_archive_configured(ArchiveModuleState *state)
#else
walg_archive_configured(void)
#endif
{	
	// Check if the file descriptor is not an empty.
	if (walg_socket == NULL || walg_socket[0] == '\0'){
		ereport(ERROR,
				errcode_for_file_access(),
				errmsg("\"walg_archive.walg_socket\" parameter from config is an empty string."));	
		return false;
	}
	
	// Set connection through file descriptor
	fd = set_connection();
	
	char message_type = 'C';
	char message_body[] = "CHECK";

	char p[sizeof(message_body) + 2];
	const uint16 message_len = sizeof(p);
	uint16 res_size = pg_hton16(message_len);

	memcpy(p, &message_type, sizeof(message_type));
	memcpy(p+1, &res_size, sizeof(uint16));
	memcpy(p+3, message_body, sizeof(message_body)-1);

	// Check that the message has been sent in full.
	ssize_t n;
	do {
		n = send(fd, p, message_len, 0);
		if (n < 0) 
		{
			ereport(ERROR,
					errcode_for_file_access(),
					errmsg("Failed to send check message."));
			return false;
		}
	} while (n != message_len);

	// Get response from the WAL-G.
	char response[512];
	if (recv(fd, &response, sizeof(response), 0) == -1)
	{
		ereport(ERROR,
				errcode_for_file_access(),
				errmsg("Failed to receive check response."));
		return false;
	} 

	// Check WAL-G response.
	if (memcmp(response, "O", 1) == 0)
	{
		return true;
	} 
	ereport(ERROR,
			errcode_for_file_access(),
			errmsg("Incorrect response: %s", response));
	return false;
}

/*
 * Create connection through UNIX-socket
 * and send the name of file to wal-g daemon.
 */
static bool 
#if PG_VERSION_NUM >= 16000
walg_archive_file(ArchiveModuleState *state, const char *file, const char *path) 
#else
walg_archive_file(const char *file, const char *path) 
#endif
{
	size_t file_len = strlen(file);
	size_t message_len = file_len + 3;

	if (message_len > UINT16_MAX)
	{
		ereport(ERROR,
				errcode_for_file_access(),
				errmsg("File name too long: %s", file));
		return false;
	}

	char header[3];
	header[0] = 'F';
	uint16 res_size = pg_hton16((uint16) message_len);
	memcpy(header + 1, &res_size, sizeof(uint16));

	struct iovec iov[2] = {
		{ header, sizeof(header) },
		{ (void *) file, file_len },
	};

	// Check that the message has been sent in full.
	ssize_t n;
	do {
		n = writev(fd, iov, 2);
		if (n < 0)
		{
			ereport(ERROR,
					errcode_for_file_access(),
					errmsg("Failed to send file message\n"));
			return false;
		}
	} while (n != message_len);

	// Get response from the WAL-G.
	char response[512];
	if (recv(fd, &response, sizeof(response), 0) == -1) 
	{	
		ereport(ERROR,
				errcode_for_file_access(),
		 		errmsg("Failed to receive message from WAL-G\n"));
		return false; 
	}

	// Check WAL-G response.
	if (memcmp(response, "O", 1) == 0) 
	{
		ereport(LOG,
				(errmsg("File: %s has been sent\n", file)));
		return true;
	}
	ereport(ERROR,
			errcode_for_file_access(),
			errmsg("Message includes error\n."));

	return false;
}
/*
 * Set connection with wal-g
 * socket and return file descriptor.
 */
static int 
set_connection(void) 
{
	int sock = socket(AF_UNIX, SOCK_STREAM, 0);
	if (sock == -1) 
	{
		ereport(ERROR,
				errcode_for_file_access(),
		 		errmsg("Error on creating of socket\n"));
		return -1;
	}
	
	struct sockaddr_un remote;
	memset(&remote, 0, sizeof(remote));
	remote.sun_family = AF_UNIX;
	strlcpy(remote.sun_path, walg_socket, sizeof(remote.sun_path));

	if (connect(sock, (struct sockaddr*)&remote, sizeof(remote)) == -1)
	{
		ereport(ERROR,
				errcode_for_file_access(),
		 		errmsg("Error on connecting to socket\n"));
		return -1;
	}
	return sock;
}