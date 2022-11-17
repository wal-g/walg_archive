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
#include "storage/copydir.h"
#include "storage/fd.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "port/pg_bswap.h"
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/un.h>
#include <setjmp.h>
#include <string.h>
#include <errno.h>

PG_MODULE_MAGIC;

void		_PG_init(void);
void		_PG_archive_module_init(ArchiveModuleCallbacks *cb);

static char *walg_socket=NULL;
static int fd;

static bool check_walg_socket(char **newval, void **extra, GucSource source);
static bool walg_archive_configured(void);
static bool walg_archive_file(const char *file, const char *path);
static int set_connection(void);

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
void
_PG_archive_module_init(ArchiveModuleCallbacks *cb)
{
	AssertVariableIsOfType(&_PG_archive_module_init, ArchiveModuleInit);
	
	cb->check_configured_cb = walg_archive_configured;
	cb->archive_file_cb = walg_archive_file;
}

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
walg_archive_configured(void)
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
	uint16 message_len = sizeof(message_body) + 2;
	uint16 res_size = pg_hton16(message_len);

	char *p = palloc(sizeof(char)*message_len);
	memcpy(p, &message_type, sizeof(message_type));
	memcpy(p+1, &res_size, sizeof(uint16));
	memcpy(p+3, message_body, sizeof(message_body)-1);

	// Check that the message has been sent in full.
	int n;
	do {
		n = send(fd, p, message_len, 0);
		if (n < 0) 
		{
			ereport(ERROR,
					errcode_for_file_access(),
					errmsg("Failed to send check message."));
			pfree(p);	
			return false;
		}
	} while (n != message_len);
	pfree(p);

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
walg_archive_file(const char *file, const char *path) 
{	
	char message_type = 'F';
	uint16 message_len = 27;
	uint16 res_size = pg_hton16(message_len);

	char *p = palloc(sizeof(char)*message_len);
	memcpy(p, &message_type, sizeof(message_type));
	memcpy(p+1, &res_size, sizeof(uint16));
	memcpy(p+3, file, 24);
	
	// Check that the message has been sent in full.
	int n;
	do {
		n = send(fd, p, message_len, 0);
		if (n < 0)
		{
			ereport(ERROR,
					errcode_for_file_access(),
					errmsg("Failed to send file message \n"));
			pfree(p);
			return false; 
		}
	} while (n != message_len);
	pfree(p);

	// Get response from the WAL-G.
	char response[512];
	if (recv(fd, &response, sizeof(response), 0) == -1) 
	{	
		printf("err : %d", errno);
		ereport(ERROR,
				errcode_for_file_access(),
		 		errmsg("Failed to receive message from WAL-G \n"));
		return false; 
	}

	// Check WAL-G response.
	if (memcmp(response, "O", 1) == 0) 
	{
		ereport(LOG,
				(errmsg("File: %s has been sent \n", file)));
    	return true;
	}
	ereport(ERROR,
			errcode_for_file_access(),
			errmsg("Message includes error \n."));

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
		 		errmsg("Error on creating of socket \n"));
		return -1;
	}
	
	struct sockaddr_un remote;
	remote.sun_family = AF_UNIX;
	
    strcpy(remote.sun_path, walg_socket);
    int data_len = strlen(remote.sun_path) + sizeof(remote.sun_family);
	if (connect(sock, (struct sockaddr*)&remote, data_len) == -1)
	{
		printf("\nError Code: %d\n", errno);
		ereport(ERROR,
				errcode_for_file_access(),
		 		errmsg("Error on connecting to socket \n"));
		return -1;
	}
	return sock;
}