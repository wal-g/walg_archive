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

	if (*newval == NULL || *newval[0] == '\0')
		return true;

	if (strlen(*newval) + 64 + 2 >= MAXPGPATH)
	{
		GUC_check_errdetail("Path ti file is too long.");
		return false;
	}

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
 * (N - 3) byte - message body, char
 */


/*
 * walg_archive_configured
 *
 * We check connection with wal-g socket
 * with sending test message
 */
static bool
walg_archive_configured(void)
{
	fd = set_connection();
	char *message_type = "C";
	char message_body[] = "CHECK";
	uint16 message_len = sizeof(message_body) + 3;
	uint16 res_size = pg_hton16(message_len);

	char *p = palloc(sizeof(char)*message_len);
	memcpy(p, message_type, sizeof(*message_type));
	memcpy(p+1, &res_size, sizeof(uint16));
	memcpy(p+3, message_body, sizeof(message_body));

	if (send(fd, p, message_len, 0 ) == -1)
	{
		return false;
	} 
	pfree(p);
	char response[512];

	if (recv(fd, &response, sizeof(response), 0) == -1)
	{
		return false;
	} 

	if (strcmp(response, "CHECKED") == 0)
	{
		return true;
	} 
	return false;
}

/*
 * Create connection through UNIX-socket
 * and send the name of file to wal-g daemon.
 *
 */
static bool 
walg_archive_file(const char *file, const char *path) 
{	
	char *message_type = "F";
	uint16 message_len = 28;
	uint16 res_size = pg_hton16(message_len);

	char *p = palloc(sizeof(char)*message_len);
	memcpy(p, message_type, sizeof(*message_type));
	memcpy(p+1, &res_size, sizeof(uint16));
	memcpy(p+3, file, 25);
	
	if (send(fd, p, message_len, 0) == -1)
	{
		ereport(ERROR,
				errcode_for_file_access(),
		 		errmsg("Error on sending message \n"));
		return false; 
	}
	pfree(p);
	char response[512];

	if (recv(fd, &response, sizeof(response), 0) == -1) 
	{	
		printf("err : %d", errno);
		ereport(ERROR,
				errcode_for_file_access(),
		 		errmsg("Error on receiving message from wal-g \n"));
		return false; 
	}
	char is_ok[3];
	memcpy(is_ok, response, 2);

	if (strcmp(is_ok, "OK") == 0) 
	{
		ereport(LOG,
			(errmsg("File: %s has been sent\n", file)));
    	return true;
	}
	ereport(ERROR,
		errcode_for_file_access(),
		errmsg("Error code: %s \n", response));

    return false;
}
/*
 * Set connection with wal-g
 * socket and return fd
 */
static int 
set_connection(void) 
{
	int sock = socket(AF_UNIX, SOCK_STREAM, 0);
	if (sock == -1) {
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