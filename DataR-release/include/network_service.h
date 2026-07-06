#ifndef _NETWORK_SERVICE_H_
#define _NETWORK_SERVICE_H_

void ServerProcess(void);
int ReplyToClient(int client_fd, const char *message);
#endif