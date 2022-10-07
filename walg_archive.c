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

static char WalPushSocketName[] = "/tmp/wal-push.sock";
static int fd;

static bool walg_archive_configured(void);
static bool walg_archive_file(const char *file, const char *path);
static int set_connection();

/*
 * _PG_init
 *
 * Defines the module's GUC.
 */
void
_PG_init(void)
{
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
 * walg_archive_configured
 *
 * We check connection with wal-g socket
 * with sending test message
 */
static bool
walg_archive_configured(void)
{
	fd = set_connection();
	char *check_message = "CHECK";
	if (send(fd, check_message, strlen(check_message)*sizeof(char), 0 ) == -1)
	{
		return false;
	} 
	char check_response[512];

	if (recv(fd, &check_response, sizeof(check_response), 0) == -1)
	{
		return false;
	} 

	if (strcmp(check_response, "CHECKOK") == 0)
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
	if (send(fd, file, strlen(file)*sizeof(char), 0 ) == -1)
	{
		ereport(ERROR,
				errcode_for_file_access(),
		 		errmsg("Error on sending message \n"));
		return false; 
	}

	char response[512];
	if (recv(fd, &response, sizeof(response), 0) == -1) 
	{	
		printf("err : %d", errno);
		ereport(ERROR,
				errcode_for_file_access(),
		 		errmsg("Error on receiving message from wal-g \n"));
		return false; 
	}
	if (strcmp(response, "OK") == 0) 
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
    strcpy(remote.sun_path, WalPushSocketName);
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