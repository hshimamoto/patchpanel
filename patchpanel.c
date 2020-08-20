// MIT License Copyright (c) 2020 Hiroshi Shimamoto
#include <sys/time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>

int listensocket(char *arg)
{
	struct sockaddr_in addr;
	int s, one = 1;
	int port = atoi(arg + 1); // just skip ':'

	s = socket(AF_INET, SOCK_STREAM, 0);
	if (s < 0)
		return -1;
	if (setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0)
		goto bad;
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = INADDR_ANY;
	addr.sin_port = htons(port);
	if (bind(s, (struct sockaddr *)&addr, sizeof(addr)) < 0)
		goto bad;
	if (listen(s, 5) < 0)
		goto bad;

	return s;
bad:
	close(s);
	return -1;
}

struct link {
	char name[256];
	int sock;
	struct timeval tv; // last
	// temporary buffer
	char buf[256];
	int sz;
};

struct stream {
	struct link *link; // correspond link
	int connected;
	int left, right; // socket for Left side and Right side
	struct timeval ltv, rtv; // last
};

// global
#define MAX_LINKS (256)
#define MAX_STREAMS (256)
struct link links[MAX_LINKS];
struct stream streams[MAX_STREAMS];

struct link *find_emptylink(void)
{
	for (int i = 0; i < MAX_LINKS; i++) {
		if (links[i].name[0] == 0)
			return &links[i];
	}
	return NULL;
}

struct link *find_link(char *name)
{
	for (int i = 0; i < MAX_LINKS; i++) {
		if (strcmp(links[i].name, name) == 0)
			return &links[i];
	}
	return NULL;
}

struct stream *find_emptystream(void)
{
	for (int i = 0; i < MAX_STREAMS; i++) {
		if (streams[i].link == NULL)
			return &streams[i];
	}
	return NULL;
}

struct stream *find_stream(char *name)
{
	for (int i = 0; i < MAX_STREAMS; i++) {
		if (streams[i].link == NULL || streams[i].right != -1)
			continue;
		if (strcmp(streams[i].link->name, name) == 0)
			return &streams[i];
	}
	return NULL;
}

void new_connection(int s)
{
	struct sockaddr_in addr;
	socklen_t len = sizeof(addr);

	int sock = accept(s, (struct sockaddr *)&addr, &len);
	if (sock < 0)
		return;
	printf("accepted %d\n", sock);

	struct link *lnk = find_emptylink();
	if (lnk == NULL) {
		printf("link slot full\n");
		close(sock);
		return;
	}
	// mark temporary
	lnk->name[0] = 1;
	lnk->sock = sock;
	lnk->sz = 0;
	gettimeofday(&lnk->tv, NULL);
}

void handle_request(struct link *lnk)
{
	gettimeofday(&lnk->tv, NULL); // mark

	int rest = 255 - lnk->sz;
	int ret = read(lnk->sock, &lnk->buf[lnk->sz], rest);

	if (ret <= 0) {
		// free it
		lnk->name[0] = 0;
		close(lnk->sock);
		lnk->sock = -1;
		return;
	}

	lnk->sz += ret;
	lnk->buf[lnk->sz] = 0;

	//printf("%d read [%s]\n", lnk->sock, lnk->buf);

	if (strncmp(lnk->buf, "LINK ", 5) == 0) {
		int ok = 0;
		for (int i = 5; i < lnk->sz - 1; i++) {
			if (lnk->buf[i] == '\r' && lnk->buf[i+1] == '\n') {
				ok = 1;
				lnk->name[i-5] = 0;
				break;
			}
			lnk->name[i-5] = lnk->buf[i];
		}
		if (ok == 0) {
			lnk->name[0] = 1;
			return;
		}
		printf("LINK %s\n", lnk->name);
		// clear
		lnk->sz = 0;
		return;
	}
	if (strncmp(lnk->buf, "CONNECTED ", 10) == 0) {
		int ok = 0;
		for (int i = 10; i < lnk->sz - 1; i++) {
			if (lnk->buf[i] == '\r' && lnk->buf[i+1] == '\n') {
				ok = 1;
				lnk->name[i-10] = 0;
				break;
			}
			lnk->name[i-10] = lnk->buf[i];
		}
		if (ok == 0) {
			lnk->name[0] = 1;
			return;
		}
		printf("CONNECTED %s\n", lnk->name);
		// connect to stream
		struct stream *strm = find_stream(lnk->name);
		if (strm == NULL) {
			printf("no waiting stream %s\n", lnk->name);
			close(lnk->sock);
			lnk->name[0] = 0;
			lnk->sock = -1;
			return;
		}
		strm->right = lnk->sock;
		gettimeofday(&strm->rtv, NULL);
		strm->connected = 1;
		lnk->name[0] = 0;
		lnk->sock = -1;
		lnk->sz = 0;
		return;
	}
	// CONNECT METHOD
	if (strncmp(lnk->buf, "CONNECT ", 8) == 0) {
		int ok = 0;
		for (int i = 8; i < lnk->sz - 3; i++) {
			if (lnk->buf[i] == '\r' && lnk->buf[i+1] == '\n' &&
			    lnk->buf[i+2] == '\r' && lnk->buf[i+3] == '\n') {
				ok = 1;
				break;
			}
			// CONNECT host:port HTTP ... CRLF CRLF
			if (lnk->buf[i] == ':')
				lnk->name[i-8] = 0;
			else
				lnk->name[i-8] = lnk->buf[i];
		}
		if (ok == 0) {
			lnk->name[0] = 1;
			return;
		}
		char name[256];
		strcpy(name, lnk->name);
		// clear to prevent that it hit in search
		lnk->name[0] = 0;
		printf("CONNECT %s\n", name);
		// create stream
		struct stream *strm = find_emptystream();
		char *resp = "HTTP/1.0 400 Bad Request\r\n\r\n";
		// keep sock variable here to handle error and correct
		int sock = lnk->sock;
		if (strm == NULL) {
			printf("no empty stream slot\n");
			goto out;
		}
		struct link *rlnk = find_link(name);
		if (rlnk == NULL) {
			printf("no such link %s\n", lnk->name);
			goto out;
		}
		strm->link = rlnk;
		strm->left = sock;
		strm->right = -1;
		strm->connected = 0;
		gettimeofday(&strm->ltv, NULL);
		// sock has been passed to stream
		// prevent close at end
		lnk->sock = -1;
		// send request
		printf("request to %s %d\n", rlnk->name, rlnk->sock);
		write(rlnk->sock, "NEW\r\n", 5);
		// ok
		resp = "HTTP/1.0 200 Established\r\n\r\n";
out:
		write(sock, resp, strlen(resp));
		close(lnk->sock);
		lnk->name[0] = 0;
		lnk->sock = -1;
		lnk->sz = 0;
		return;
	}
	// Link keep alive
	if (strncmp(lnk->buf, "KeepAlive\r\n", 11) == 0) {
		lnk->sz = 0;
		return;
	}
}

void stream_left(struct stream *strm)
{
	gettimeofday(&strm->ltv, NULL);
	char buf[4096];
	int ret = read(strm->left, buf, 4096);
	if (ret <= 0) {
		// will close
		printf("stream %s close left\n", strm->link->name);
		close(strm->left);
		strm->left = -1;
		close(strm->right);
		strm->right = -1;
		return;
	}
	// forward to right
	if (strm->right >= 0)
		write(strm->right, buf, ret);
}

void stream_right(struct stream *strm)
{
	gettimeofday(&strm->rtv, NULL);
	char buf[4096];
	int ret = read(strm->right, buf, 4096);
	if (ret <= 0) {
		// will close
		printf("stream %s close right\n", strm->link->name);
		close(strm->right);
		strm->right = -1;
		close(strm->left);
		strm->left = -1;
		return;
	}
	// forward to left
	if (strm->left >= 0)
		write(strm->left, buf, ret);
}

void mainloop(int s)
{
	fd_set fds;
	int max;

	FD_ZERO(&fds);
	FD_SET(s, &fds);
	max = s;
	for (int i = 0; i < MAX_LINKS; i++) {
		if (links[i].name[0] == 0)
			continue;
		FD_SET(links[i].sock, &fds);
		if (links[i].sock > max)
			max = links[i].sock;
	}
	for (int i = 0; i < MAX_STREAMS; i++) {
		struct stream *strm = &streams[i];
		if (strm->link == NULL)
			continue;
		if (strm->connected == 0)
			continue;
		int left = strm->left;
		int right = strm->right;
		if (left >= 0) {
			FD_SET(left, &fds);
			if (left > max)
				max = left;
		}
		if (right >= 0) {
			FD_SET(right, &fds);
			if (right > max)
				max = right;
		}
	}
	max += 1;

	struct timeval tv;
	tv.tv_sec = 60;
	tv.tv_usec = 0;

	int ret = select(max, &fds, NULL, NULL, NULL);
	if (ret < 0)
		return;

	// accept?
	if (FD_ISSET(s, &fds))
		new_connection(s);

	gettimeofday(&tv, NULL);

	for (int i = 0; i < MAX_LINKS; i++) {
		struct link *lnk = &links[i];
		if (lnk->name[0] == 0)
			continue;
		if (FD_ISSET(lnk->sock, &fds)) {
			handle_request(lnk);
			return;
		}
	}
	for (int i = 0; i < MAX_STREAMS; i++) {
		struct stream *strm = &streams[i];
		if (strm->link == NULL)
			continue;
		if (strm->connected == 0)
			continue;
		int left = strm->left;
		int right = strm->right;
		if (left >= 0 && FD_ISSET(left, &fds))
			stream_left(strm);
		if (right >= 0 && FD_ISSET(right, &fds))
			stream_right(strm);
	}
	for (int i = 0; i < MAX_STREAMS; i++) {
		struct stream *strm = &streams[i];
		if (strm->link == NULL)
			continue;
		if (strm->left == -1 && strm->right == -1) {
			printf("close stream %s\n", strm->link->name);
			strm->link = NULL;
			strm->connected = 0;
			continue;
		}
		// check last recv
		int disconnect = 0;
		if (strm->left >= 0) {
			if ((tv.tv_sec - strm->ltv.tv_sec) > 300)
				disconnect = 1;
		}
		if (strm->right >= 0) {
			if ((tv.tv_sec - strm->rtv.tv_sec) > 300)
				disconnect = 1;
		}
		if (disconnect == 0)
			continue;
		printf("no activity %s\n", strm->link->name);
		close(strm->left);
		strm->left = -1;
		close(strm->right);
		strm->right = -1;
		strm->link = NULL;
		strm->connected = 0;
	}
}

void init(void)
{
	for (int i = 0; i < MAX_LINKS; i++) {
		links[i].name[0] = 0;
		links[i].sock = -1;
	}
	for (int i = 0; i < MAX_STREAMS; i++) {
		streams[i].link = NULL;
		streams[i].connected = 0;
		streams[i].left = -1;
		streams[i].right = -1;
	}
}

int main(int argc, char **argv)
{
	setbuf(stdout, NULL);

	char *laddr = ":8800";
	if (argc > 1)
		laddr = argv[1];
	printf("start patchpanel %s\n", laddr);

	int sock = listensocket(laddr);
	if (sock < 0)
		return 1;

	init();
	for (;;)
		mainloop(sock);

	return 0;
}
