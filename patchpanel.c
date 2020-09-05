// MIT License Copyright (c) 2020 Hiroshi Shimamoto
#include <sys/time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <errno.h>

static inline void ldatetime(char *dt, int sz)
{
	time_t t = time(NULL);
	struct tm *tmp = localtime(&t);
	if (!tmp)
		strcpy(dt, "-");
	else
		strftime(dt, sz, "%F %T", tmp);
}

#define logf(...) \
	do { \
		char dt[80]; \
		ldatetime(dt, sizeof(dt)); \
		fprintf(stderr, "%s ", dt); \
		fprintf(stderr, __VA_ARGS__); \
		fflush(stderr); \
	} while (0)

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
	struct timeval tv_linked; // start of this link
	// temporary buffer
	char buf[256];
	int sz;
};

struct stream {
	int used; // used flag
	char name[256]; // keep name
	int connected;
	int left, right; // socket for Left side and Right side
	struct timeval tv;
	// statistics
	struct timeval tv_est; // established
	long bytes_l2r, bytes_r2l; // transfer size
};

// global
#define MAX_LINKS (256)
#define MAX_STREAMS (256)
#define NO_COMMAND_TIME   (100) // 100sec
#define NO_CONNECTED_TIME (10)  // 10sec
#define NO_ACTIVITY_TIME  (8 * 60 * 60) // 8h
#define STATS_INTERVAL (3600)
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
		if (streams[i].used == 0)
			return &streams[i];
	}
	return NULL;
}

struct stream *find_stream(char *name)
{
	for (int i = 0; i < MAX_STREAMS; i++) {
		if (streams[i].used == 0 || streams[i].right != -1)
			continue;
		if (strcmp(streams[i].name, name) == 0)
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
	logf("accepted %d from %s\n", sock, inet_ntoa(addr.sin_addr));

	int optval;
	socklen_t optlen = sizeof(optval);

	optval = 1;
	if (setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, &optval, optlen) < 0)
		logf("set keepalive failed: %d\n", errno);
	optval = 60;
	if (setsockopt(sock, SOL_TCP, TCP_KEEPIDLE, &optval, optlen) < 0)
		logf("set keepalive: keepidle failed: %d\n", errno);
	optval = 6;
	if (setsockopt(sock, SOL_TCP, TCP_KEEPCNT, &optval, optlen) < 0)
		logf("set keepalive: keepcnt failed: %d\n", errno);
	optval = 10;
	if (setsockopt(sock, SOL_TCP, TCP_KEEPINTVL, &optval, optlen) < 0)
		logf("set keepalive: keepintvl failed: %d\n", errno);

	struct link *lnk = find_emptylink();
	if (lnk == NULL) {
		logf("link slot full\n");
		close(sock);
		return;
	}
	// mark temporary
	lnk->name[0] = 1;
	lnk->sock = sock;
	lnk->sz = 0;
	gettimeofday(&lnk->tv, NULL);
	gettimeofday(&lnk->tv_linked, NULL);
}

void get_duration(char *buf, int n, struct timeval *prev)
{
	struct timeval now;
	gettimeofday(&now, NULL);
	int duration = now.tv_sec - prev->tv_sec;
	if (duration < 600) {
		int ms = (now.tv_usec - prev->tv_usec) / 1000;
		if (ms < 0) {
			ms += 1000;
			duration++;
		}
		snprintf(buf, 32, "%d.%03ds", duration, ms);
	} else if (duration < 3600) {
		snprintf(buf, 32, "%dm", duration / 60);
	} else if (duration < 12 * 3600) {
		int h = duration / 3600;
		int m = (duration / 60) % 60;
		snprintf(buf, 32, "%dh %dm", h, m);
	} else {
		snprintf(buf, 32, "%dh", duration / 3600);
	}
}

void close_link(struct link *lnk)
{
	int sock = lnk->sock;
	lnk->sock = -1;

	if (sock == -1)
		goto out;

	char *name = lnk->name;
	if (name[0] == 0 || name[0] == 1)
		name = "-";
	char buf[32];
	get_duration(buf, 32, &lnk->tv_linked);
	logf("close_link %s %d [%s]\n", name, sock, buf);
	close(sock);
out:
	lnk->name[0] = 0;
}

void close_stream(struct stream *strm)
{
	char buf[32];
	get_duration(buf, 32, &strm->tv_est);
	logf("close_stream %s left %d right %d [%s] %ld <=> %ld\n",
			strm->name, strm->left, strm->right, buf,
			strm->bytes_r2l, strm->bytes_l2r);
	close(strm->left);
	close(strm->right);
	strm->left = -1;
	strm->right = -1;
	strm->used = 0;
	strm->connected = 0;
}

void handle_request(struct link *lnk)
{
	gettimeofday(&lnk->tv, NULL); // mark

	int rest = 255 - lnk->sz;
	int ret = read(lnk->sock, &lnk->buf[lnk->sz], rest);

	if (ret <= 0) {
		close_link(lnk);
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
		logf("LINK %s\n", lnk->name);
		// clear
		lnk->sz = 0;
		// check OLD links
		for (int i = 0; i < MAX_LINKS; i++) {
			struct link *tmp = &links[i];
			if (lnk == tmp)
				continue;
			if (strcmp(lnk->name, tmp->name))
				continue;
			logf("mark %s %d old\n", tmp->name, tmp->sock);
			tmp->name[0] = '~';
		}
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
		logf("CONNECTED %s\n", lnk->name);
		// connect to stream
		struct stream *strm = find_stream(lnk->name);
		if (strm == NULL) {
			logf("no waiting stream for %s\n", lnk->name);
			close(lnk->sock);
			lnk->name[0] = 0;
			lnk->sock = -1;
			return;
		}
		strm->right = lnk->sock;
		gettimeofday(&strm->tv, NULL);
		strm->connected = 1;
		gettimeofday(&strm->tv_est, NULL);
		logf("stream is established %s left %d right %d\n",
				strm->name, strm->left, strm->right);
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
		// keep sock variable here to handle error and correct
		int sock = lnk->sock;
		char *resp = "HTTP/1.0 400 Bad Request\r\n\r\n";
		// create stream
		struct stream *strm = find_emptystream();
		if (strm == NULL) {
			logf("no empty stream slot\n");
			goto out;
		}
		// copy request link name
		strncpy(strm->name, lnk->name, 256);
		logf("CONNECT %s\n", strm->name);
		// clear to prevent that it hit in search
		lnk->name[0] = 0;
		struct link *rlnk = find_link(strm->name);
		if (rlnk == NULL) {
			logf("no such link %s\n", strm->name);
			resp = "HTTP/1.0 404 Not found\r\n\r\n";
			goto out;
		}
		strm->used = 1;
		strm->left = sock;
		strm->right = -1;
		strm->connected = 0;
		gettimeofday(&strm->tv, NULL);
		strm->bytes_l2r = 0;
		strm->bytes_r2l = 0;
		// sock has been passed to stream
		// prevent close at end
		lnk->sock = -1;
		// send request
		logf("request to %s %d\n", rlnk->name, rlnk->sock);
		write(rlnk->sock, "NEW\r\n", 5);
		// ok
		resp = "HTTP/1.0 200 Established\r\n\r\n";
out:
		write(sock, resp, strlen(resp));
		close_link(lnk);
		return;
	}
	// Link keep alive
	if (strncmp(lnk->buf, "KeepAlive\r\n", 11) == 0) {
		lnk->sz = 0;
		return;
	}
	// unknown?
	char *crlf = strstr(lnk->buf, "\r\n");
	if (crlf != NULL) {
		*crlf = 0;
		logf("%d unknown command %s\n", lnk->sock, lnk->buf);
		close_link(lnk);
		return;
	}
}

int transfer(struct stream *strm, int rfd, int wfd)
{
	char buf[4096];
	int r = read(rfd, buf, 4096);
	if (r <= 0)
		return -1;
	int w = write(wfd, buf, r);
	if (w <= 0)
		return -1;
	gettimeofday(&strm->tv, NULL);
	return w;
}

void stream_left(struct stream *strm)
{
	if (strm->right < 0)
		return;
	// forward to right
	int bytes = transfer(strm, strm->left, strm->right);
	if (bytes == -1) {
		logf("stream %s close left\n", strm->name);
		close_stream(strm);
		return;
	}
	strm->bytes_l2r += bytes;
}

void stream_right(struct stream *strm)
{
	if (strm->left < 0)
		return;
	// forward to right
	int bytes = transfer(strm, strm->right, strm->left);
	if (bytes == -1) {
		logf("stream %s close right\n", strm->name);
		close_stream(strm);
		return;
	}
	strm->bytes_r2l += bytes;
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
		if (strm->used == 0)
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

	int ret = select(max, &fds, NULL, NULL, &tv);
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
			continue;
		}
	}
	for (int i = 0; i < MAX_LINKS; i++) {
		struct link *lnk = &links[i];
		if (lnk->name[0] == 0)
			continue;
		if ((tv.tv_sec - lnk->tv.tv_sec) > NO_COMMAND_TIME) {
			char *name = "-";
			if (lnk->name[0] != 1) {
				name = lnk->name;
			}
			logf("no command from %s %d\n", name, lnk->sock);
			close_link(lnk);
			continue;
		}
	}
	for (int i = 0; i < MAX_STREAMS; i++) {
		struct stream *strm = &streams[i];
		if (strm->used == 0)
			continue;
		if (strm->connected == 0)
			continue;
		int left = strm->left;
		if (left >= 0 && FD_ISSET(left, &fds))
			stream_left(strm);
		int right = strm->right;
		if (right >= 0 && FD_ISSET(right, &fds))
			stream_right(strm);
	}
	for (int i = 0; i < MAX_STREAMS; i++) {
		struct stream *strm = &streams[i];
		if (strm->used == 0)
			continue;
		if (strm->left == -1 && strm->right == -1) {
			// should not happen
			logf("stream %s disconnected\n", strm->name);
			strm->used = 0;
			strm->connected = 0;
			continue;
		}
		// check from last recv
		int timeout = strm->connected ? NO_ACTIVITY_TIME : NO_CONNECTED_TIME;
		int disconnect = 0;
		if (strm->left >= 0 || strm->right >= 0) {
			if ((tv.tv_sec - strm->tv.tv_sec) > timeout)
				disconnect = 1;
		}
		if (disconnect == 0)
			continue;
		logf("no activity %s\n", strm->name);
		close_stream(strm);
	}
}

void init(void)
{
	for (int i = 0; i < MAX_LINKS; i++) {
		links[i].name[0] = 0;
		links[i].sock = -1;
	}
	for (int i = 0; i < MAX_STREAMS; i++) {
		streams[i].used = 0;
		streams[i].connected = 0;
		streams[i].left = -1;
		streams[i].right = -1;
	}
}

void show_stats(void)
{
	int nr_links = 0, nr_streams = 0;

	for (int i = 0; i < MAX_LINKS; i++) {
		struct link *lnk = &links[i];
		if (lnk->name[0] == 0)
			continue;
		nr_links++;
	}
	for (int i = 0; i < MAX_STREAMS; i++) {
		struct stream *strm = &streams[i];
		if (strm->used == 0)
			continue;
		nr_streams++;
	}
	logf("stats %d links %d streams\n", nr_links, nr_streams);
}

int main(int argc, char **argv)
{
	char *laddr = ":8800";
	if (argc > 1)
		laddr = argv[1];
	logf("start patchpanel %s\n", laddr);

	signal(SIGPIPE, SIG_IGN);

	int sock = listensocket(laddr);
	if (sock < 0)
		return 1;

	init();

	struct timeval tv_stats;
	gettimeofday(&tv_stats, NULL);
	for (;;) {
		struct timeval now;
		gettimeofday(&now, NULL);

		if ((now.tv_sec - tv_stats.tv_sec) > STATS_INTERVAL) {
			show_stats();
			tv_stats = now;
		}
		mainloop(sock);
	}

	return 0;
}
