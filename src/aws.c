// SPDX-License-Identifier: BSD-3-Clause

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/sendfile.h>
#include <sys/eventfd.h>
#include <libaio.h>
#include <errno.h>

#include "aws.h"
#include "utils/util.h"
#include "utils/debug.h"
#include "utils/sock_util.h"
#include "utils/w_epoll.h"

/* server socket file descriptor */
static int listenfd;

/* epoll file descriptor */
static int epollfd;

static io_context_t ctx;

static int aws_on_path_cb(http_parser *p, const char *buf, size_t len)
{
	struct connection *conn = (struct connection *)p->data;

	memcpy(conn->request_path, buf, len);
	conn->request_path[len] = '\0';
	conn->have_path = 1;

	return 0;
}

static void connection_prepare_send_reply_header(struct connection *conn)
{
	/* TODO: Prepare the connection buffer to send the reply header. */
	struct stat file_info;
	int rc;

	rc = fstat(conn->fd, &file_info);
	DIE(rc < 0, "fstat");

	conn->file_size = file_info.st_size;

	memset(conn->send_buffer, 0, BUFSIZ);

	sprintf(conn->send_buffer,
		"HTTP/1.1 200 OK\r\n"
		"Accept-Ranges: bytes\r\n"
		"Content-Length: %ld\r\n"
		"Vary: Accept-Encoding\r\n"
		"Connection: close\r\n"
		"\r\n",
		conn->file_size);

	conn->send_len = strlen(conn->send_buffer);
}

static void connection_prepare_send_404(struct connection *conn)
{
	/* TODO: Prepare the connection buffer to send the 404 header. */

	memset(conn->send_buffer, 0, BUFSIZ);
	sprintf(conn->send_buffer,
		"HTTP/1.1 404\r\n"
		"Content-Length: 0\r\n"
		"Connection: close\r\n"
		"\r\n");

	conn->send_len = strlen(conn->send_buffer);
}

static enum resource_type connection_get_resource_type(struct connection *conn)
{
	/* TODO: Get resource type depending on request path/filename. Filename should
	 * point to the static or dynamic folder.
	 */
	sprintf(conn->filename, "%s", AWS_DOCUMENT_ROOT);
	memcpy(conn->filename+strlen(AWS_DOCUMENT_ROOT), conn->request_path, sizeof(conn->request_path));
	if (strstr(conn->filename, AWS_REL_STATIC_FOLDER) != 0) {
		conn->res_type = RESOURCE_TYPE_STATIC;
		return RESOURCE_TYPE_STATIC;
	}
	if (strstr(conn->filename, AWS_REL_DYNAMIC_FOLDER) != 0) {
		conn->res_type = RESOURCE_TYPE_DYNAMIC;
		return RESOURCE_TYPE_DYNAMIC;
	}

	return RESOURCE_TYPE_NONE;
}


struct connection *connection_create(int sockfd)
{
	/* TODO: Initialize connection structure on given socket. */
	struct connection *conn = malloc(sizeof(*conn));

	DIE(conn == NULL, "malloc");

	conn->sockfd = sockfd;

	conn->ctx = ctx;
	conn->eventfd = eventfd(0, EFD_NONBLOCK);

	conn->state = STATE_INITIAL;

	memset(conn->filename, 0, BUFSIZ);
	memset(conn->recv_buffer, 0, BUFSIZ);
	memset(conn->send_buffer, 0, BUFSIZ);
	memset(conn->request_path, 0, BUFSIZ);

	return conn;
}

void connection_start_async_io(struct connection *conn)
{
	/* TODO: Start asynchronous operation (read from file).
	 * Use io_submit(2) & friends for reading data asynchronously.
	 */
	int rc;

	conn->eventfd = eventfd(0, EFD_NONBLOCK);
	conn->dynamic_buffer = malloc(conn->file_size);
	memset(conn->dynamic_buffer, 0, conn->file_size);

	memset(&conn->iocb, 0, sizeof(conn->iocb));
	conn->piocb[0] = &conn->iocb;

	io_prep_pread(&conn->iocb, conn->fd, conn->dynamic_buffer, conn->file_size, 0);
	io_set_eventfd(&conn->iocb, conn->eventfd);
	conn->state = STATE_SENDING_DATA;
	rc = io_submit(conn->ctx, 1, conn->piocb);
	DIE(rc < 0, "io_submit");

	w_epoll_add_ptr_out(epollfd, conn->eventfd, conn);
}

void connection_remove(struct connection *conn)
{
	/* TODO: Remove connection handler. */
	close(conn->sockfd);
	close(conn->fd);
	conn->state = STATE_CONNECTION_CLOSED;
	free(conn);
}

void handle_new_connection(void)
{
	/* TODO: Handle a new connection request on the server socket. */
	static int sockfd;
	socklen_t addrlen = sizeof(struct sockaddr_in);
	struct sockaddr_in addr;
	struct connection *conn;
	int rc;

	/* TODO: Accept new connection. */
	sockfd = accept(listenfd, (SSA *) &addr, &addrlen);
	DIE(sockfd < 0, "accept");

	dlog(LOG_ERR, "Accepted connection from: %s:%d\n",
		inet_ntoa(addr.sin_addr), ntohs(addr.sin_port));

	/* TODO: Set socket to be non-blocking. */
	rc = fcntl(sockfd, F_SETFL, fcntl(sockfd, F_GETFL, 0) | O_NONBLOCK);
	DIE(rc < 0, "set to non-blocking");

	/* TODO: Instantiate new connection handler. */
	conn = connection_create(sockfd);

	/* TODO: Add socket to epoll. */
	rc = w_epoll_add_ptr_in(epollfd, sockfd, conn);
	DIE(rc < 0, "w_epoll_add_in");

	/* TODO: Initialize HTTP_REQUEST parser. */
	http_parser_init(&(conn->request_parser), HTTP_REQUEST);
	conn->request_parser.data = conn;
}

void receive_data(struct connection *conn)
{
	/* TODO: Receive message on socket.
	 * Store message in recv_buffer in struct connection.
	 */
	conn->state = STATE_RECEIVING_DATA;

	ssize_t bytes_recv;
	ssize_t total_recv = 0;
	int rc;
	char abuffer[64];

	rc = get_peer_address(conn->sockfd, abuffer, 64);
	if (rc < 0) {
		ERR("get_peer_address");
		goto remove_connection;
	}
	errno = 0;
	while (1) {
		bytes_recv = recv(conn->sockfd, conn->recv_buffer + total_recv, BUFSIZ - total_recv, 0);

		if (bytes_recv == -1) {
			if (errno == EWOULDBLOCK) // Socket would block, no more data to be received
				break;
			goto remove_connection;
		}

		if (bytes_recv < 0) {
			dlog(LOG_ERR, "Error in communication from: %s\n", abuffer);
			goto remove_connection;
		}

		if (!total_recv && !bytes_recv) {
			dlog(LOG_INFO, "Connection closed from: %s\n", abuffer);
			goto remove_connection;
		}

		if (!bytes_recv)
			break;
		total_recv += bytes_recv;
	}

	dlog(LOG_INFO, "Received message from: %s\n", abuffer);
	dlog(LOG_INFO, "Message: %s\n", conn->recv_buffer);
	printf("--\n%s--\n", conn->recv_buffer);

	conn->recv_len = total_recv;
	conn->state = STATE_REQUEST_RECEIVED;

	return;

remove_connection:
	rc = w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
	DIE(rc < 0, "w_epoll_remove_ptr");

	/* remove current connection */
	connection_remove(conn);
}

int connection_open_file(struct connection *conn)
{
	/* TODO: Open file and update connection fields. */
	conn->fd = open(conn->filename, O_RDONLY);
	return conn->fd;
}

void connection_complete_async_io(struct connection *conn)
{
	/* TODO: Complete asynchronous operation; operation returns successfully.
	 * Prepare socket for sending.
	 */
}

int parse_header(struct connection *conn)
{
	size_t bytes_parsed;

	/* TODO: Parse the HTTP header and extract the file path. */
	/* Use mostly null settings except for on_path callback. */
	http_parser_settings settings_on_path = {
		.on_message_begin = 0,
		.on_header_field = 0,
		.on_header_value = 0,
		.on_path = aws_on_path_cb,
		.on_url = 0,
		.on_fragment = 0,
		.on_query_string = 0,
		.on_body = 0,
		.on_headers_complete = 0,
		.on_message_complete = 0
	};

	bytes_parsed = http_parser_execute(&(conn->request_parser), &settings_on_path, conn->recv_buffer, conn->recv_len);
	printf("Parsed simple HTTP request (bytes: %lu), path: %s\n", bytes_parsed, conn->request_path);

	conn->state = STATE_SENDING_DATA;
	return 0;
}

enum connection_state connection_send_static(struct connection *conn)
{
	/* TODO: Send static data using sendfile(2). */
	ssize_t bytes_sent;
	ssize_t total_sent = 0;
	char abuffer[64];
	int rc;

	rc = get_peer_address(conn->sockfd, abuffer, 64);
	if (rc < 0) {
		ERR("get_peer_address");
		goto remove_connection;
	}

	conn->state = STATE_SENDING_DATA;

	while (total_sent < conn->file_size) {
		bytes_sent = sendfile(conn->sockfd, conn->fd, NULL, conn->file_size);
		// printf("%ld\n", bytes_sent);

		if (bytes_sent < 0) {
			dlog(LOG_ERR, "Error in communication from: %s\n", abuffer);
			goto remove_connection;
		}
		if (!bytes_sent) {
			dlog(LOG_INFO, "Connection closed from: %s\n", abuffer);
			goto remove_connection;
		}

		total_sent += bytes_sent;
		conn->file_pos += bytes_sent;
	}

	dlog(LOG_INFO, "Data sent\n");

	conn->state = STATE_DATA_SENT;

	return STATE_DATA_SENT;

remove_connection:
	rc = w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
	DIE(rc < 0, "w_epoll_remove_ptr");

	/* remove current connection */
	connection_remove(conn);

	return STATE_NO_STATE;
}

int connection_send_data(struct connection *conn)
{
	/* May be used as a helper function. */
	/* TODO: Send as much data as possible from the connection send buffer.
	 * Returns the number of bytes sent or -1 if an error occurred
	 */
	ssize_t bytes_sent;
	ssize_t total_sent = 0;
	char abuffer[64];
	int rc;

	rc = get_peer_address(conn->sockfd, abuffer, 64);
	if (rc < 0) {
		ERR("get_peer_address");
		goto remove_connection;
	}

	while (total_sent < conn->send_len) {
		bytes_sent = send(conn->sockfd, conn->send_buffer + total_sent, conn->send_len - total_sent, 0);

		if (bytes_sent < 0) {
			dlog(LOG_ERR, "Error in communication from: %s\n", abuffer);
			goto remove_connection;
		}
		if (!bytes_sent) {
			dlog(LOG_INFO, "Connection closed from: %s\n", abuffer);
			goto remove_connection;
		}

		total_sent += bytes_sent;
	}
	conn->state = STATE_HEADER_SENT;

	if (total_sent)
		return total_sent;
	return -1;

remove_connection:
	rc = w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
	DIE(rc < 0, "w_epoll_remove_ptr");

	/* remove current connection */
	connection_remove(conn);

	return -1;
}

int connection_send_dynamic(struct connection *conn)
{
	/* TODO: Read data asynchronously.
	 * Returns 0 on success and -1 on error.
	 */

	ssize_t bytes_sent;
	ssize_t total_sent = 0;
	char abuffer[64];
	int rc;

	rc = get_peer_address(conn->sockfd, abuffer, 64);
	if (rc < 0) {
		ERR("get_peer_address");
		goto remove_connection;
	}

	while (total_sent < conn->file_size) {
		bytes_sent = send(conn->sockfd, conn->dynamic_buffer + total_sent, conn->file_size - total_sent, 0);

		if (bytes_sent < 0) {
			dlog(LOG_ERR, "Error in communication from: %s\n", abuffer);
			goto remove_connection;
		}
		if (!bytes_sent) {
			dlog(LOG_INFO, "Connection closed from: %s\n", abuffer);
			goto remove_connection;
		}

		total_sent += bytes_sent;
	}

	conn->state = STATE_DATA_SENT;
	return 0;

remove_connection:
	rc = w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
	DIE(rc < 0, "w_epoll_remove_ptr");

	rc = w_epoll_remove_ptr(epollfd, conn->eventfd, conn);
	DIE(rc < 0, "w_epoll_remove_ptr");

	/* remove current connection */
	connection_remove(conn);

	return -1;
}

void handle_input(struct connection *conn)
{
	/* TODO: Handle input information: may be a new message or notification of
	 * completion of an asynchronous I/O operation.
	 */
	int rc;
	enum resource_type res_type;

	switch (conn->state) {
	case STATE_INITIAL:
		receive_data(conn);
		parse_header(conn);
		res_type = connection_get_resource_type(conn);
		if (connection_open_file(conn) == -1)
			conn->state = STATE_SENDING_404;
		else
			conn->state = STATE_SENDING_HEADER;
		dlog(LOG_INFO, "Res_type: %d\n", res_type);
		dlog(LOG_INFO, "Filename: %s\n", conn->filename);

		rc = w_epoll_update_ptr_out(epollfd, conn->sockfd, conn);
		DIE(rc < 0, "w_epoll_update_ptr_out");
		break;
	default:
		printf("shouldn't get here %d\n", conn->state);
	}
}

void handle_output(struct connection *conn)
{
	/* TODO: Handle output information: may be a new valid requests or notification of
	 * completion of an asynchronous I/O operation or invalid requests.
	 */
	int rc;

	switch (conn->state) {
	case STATE_SENDING_HEADER:
		// prepare and send header
		connection_prepare_send_reply_header(conn);
		connection_send_data(conn);
		conn->state = STATE_HEADER_SENT;
		break;
	case STATE_SENDING_404:
		connection_prepare_send_404(conn);
		connection_send_data(conn);
		conn->state = STATE_404_SENT;
		break;
	case STATE_HEADER_SENT:
		if (conn->res_type == RESOURCE_TYPE_STATIC) {
			connection_send_static(conn);
			rc = w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
			DIE(rc < 0, "w_epoll_remove_ptr");
			/* remove current connection */
			connection_remove(conn);
		}
		if (conn->res_type == RESOURCE_TYPE_DYNAMIC)
			connection_start_async_io(conn);
		break;
	case STATE_404_SENT:
		rc = w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
		DIE(rc < 0, "w_epoll_remove_ptr");

		/* remove current connection */
		connection_remove(conn);
		break;
	case STATE_SENDING_DATA:
		printf("[0]:%c\n", conn->dynamic_buffer[0]);
		if (connection_send_dynamic(conn) == 0) {
			rc = w_epoll_remove_ptr(epollfd, conn->eventfd, conn);
			DIE(rc < 0, "w_epoll_remove_ptr");

			free(conn->dynamic_buffer);
			close(conn->eventfd);
			connection_remove(conn);
		}

		break;
	default:
		ERR("Unexpected state\n");
		exit(1);
	}
}

void handle_client(uint32_t event, struct connection *conn)
{
	/* TODO: Handle new client. There can be input and output connections.
	 * Take care of what happened at the end of a connection.
	 */
	if (event & EPOLLIN)
		handle_input(conn);
	if (event & EPOLLOUT)
		handle_output(conn);
}

int main(void)
{
	int rc;

	/* TODO: Initialize asynchronous operations. */
	memset(&ctx, 0, sizeof(ctx));
	rc = io_setup(10, &ctx);
	DIE(rc < 0, "io_setup");

	/* TODO: Initialize multiplexing. */
	epollfd = w_epoll_create();
	DIE(epollfd < 0, "w_epoll_create");

	/* TODO: Create server socket. */
	listenfd = tcp_create_listener(AWS_LISTEN_PORT,
		DEFAULT_LISTEN_BACKLOG);
	DIE(listenfd < 0, "tcp_create_listener");

	/* TODO: Add server socket to epoll object*/
	rc = w_epoll_add_fd_in(epollfd, listenfd);
	DIE(rc < 0, "w_epoll_add_fd_in");

	/* Uncomment the following line for debugging. */
	dlog(LOG_INFO, "Server waiting for connections on port %d\n", AWS_LISTEN_PORT);

	/* server main loop */
	while (1) {
		struct epoll_event rev;

		/* TODO: Wait for events. */
		rc = w_epoll_wait_infinite(epollfd, &rev);
		DIE(rc < 0, "w_epoll_wait_infinite");

		/* TODO: Switch event types; consider
		 *   - new connection requests (on server socket)
		 *   - socket communication (on connection sockets)
		 */

		if (rev.data.fd == listenfd) {
			// dlog(LOG_DEBUG, "New connection\n");
			handle_new_connection();
		} else {
			// dlog(LOG_DEBUG, "New message\n");
			handle_client(rev.events, rev.data.ptr);
		}
	}

	io_destroy(ctx);

	return 0;
}
