/*
+-----------------------------------------------------------------------------------+
|  ZMQ extension for PHP                                                            |
|  Copyright (c) 2010-2013, Mikko Koppanen <mkoppanen@php.net>                      |
|  All rights reserved.                                                             |
+-----------------------------------------------------------------------------------+
|  Redistribution and use in source and binary forms, with or without               |
|  modification, are permitted provided that the following conditions are met:      |
|     * Redistributions of source code must retain the above copyright              |
|       notice, this list of conditions and the following disclaimer.               |
|     * Redistributions in binary form must reproduce the above copyright           |
|       notice, this list of conditions and the following disclaimer in the         |
|       documentation and/or other materials provided with the distribution.        |
|     * Neither the name of the copyright holder nor the                            |
|       names of its contributors may be used to endorse or promote products        |
|       derived from this software without specific prior written permission.       |
+-----------------------------------------------------------------------------------+
|  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND  |
|  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED    |
|  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE           |
|  DISCLAIMED. IN NO EVENT SHALL MIKKO KOPPANEN BE LIABLE FOR ANY                   |
|  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES       |
|  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;     |
|  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND      |
|  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT       |
|  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS    |
|  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.                     |
+-----------------------------------------------------------------------------------+
*/

#include "php_zmq.h"
#include "php_zmq_private.h"
#include "php_zmq_pollset.h"

ZEND_DECLARE_MODULE_GLOBALS(php_zmq);

static zend_class_entry *php_zmq_sc_entry;
static zend_class_entry *php_zmq_context_sc_entry;
static zend_class_entry *php_zmq_socket_sc_entry;
static zend_class_entry *php_zmq_poll_sc_entry;
static zend_class_entry *php_zmq_device_sc_entry;

static zend_class_entry *php_zmq_exception_sc_entry;
static zend_class_entry *php_zmq_context_exception_sc_entry;
static zend_class_entry *php_zmq_socket_exception_sc_entry;
static zend_class_entry *php_zmq_poll_exception_sc_entry;
static zend_class_entry *php_zmq_device_exception_sc_entry;

static zend_object_handlers zmq_object_handlers;
static zend_object_handlers zmq_socket_object_handlers;
static zend_object_handlers zmq_context_object_handlers;
static zend_object_handlers zmq_poll_object_handlers;
static zend_object_handlers zmq_device_object_handlers;

#ifdef HAVE_CZMQ_2
static zend_class_entry *php_zmq_cert_sc_entry;
static zend_class_entry *php_zmq_auth_sc_entry;

static zend_class_entry *php_zmq_cert_exception_sc_entry;
static zend_class_entry *php_zmq_auth_exception_sc_entry;

static zend_object_handlers zmq_cert_object_handlers;
static zend_object_handlers zmq_auth_object_handlers;
#endif


#include "zmq_object_access.c"

zend_class_entry *php_zmq_context_exception_sc_entry_get ()
{
	return php_zmq_context_exception_sc_entry;
}

zend_class_entry *php_zmq_socket_exception_sc_entry_get ()
{
	return php_zmq_socket_exception_sc_entry;
}

zend_class_entry *php_zmq_device_exception_sc_entry_get ()
{
	return php_zmq_device_exception_sc_entry;
}

static
zend_long php_zmq_context_socket_count_get(php_zmq_context *context) 
{
	zend_long value = 0;

	if (context->use_shared_ctx) {
		value = php_zmq_shared_ctx_socket_count();
	}
	else {
		value = context->socket_count;
	}
	return value;
}

static
void php_zmq_context_socket_count_incr(php_zmq_context *context)
{
	if (context->use_shared_ctx) {
		php_zmq_shared_ctx_socket_count_incr();
	}
	else {
		context->socket_count++;
	}
}

static
void php_zmq_context_socket_count_decr(php_zmq_context *context)
{
	if (context->use_shared_ctx) {
		php_zmq_shared_ctx_socket_count_decr();
	}
	else {
		context->socket_count--;
	}
}

/* list entries */
static int le_zmq_socket, le_zmq_context;

/** {{{ static int php_zmq_socket_list_entry(void)
*/
static int php_zmq_socket_list_entry(void)
{
	return le_zmq_socket;
}
/* }}} */

/* {{{ static int php_zmq_context_list_entry(void)
*/
static int php_zmq_context_list_entry(void)
{
	return le_zmq_context;
}
/* }}} */

/* {{{ static void php_zmq_context_destroy(php_zmq_context *context)
	Destroy the context
*/
static void php_zmq_context_destroy(php_zmq_context *context)
{
	if (context->pid == getpid()) {
		if (php_zmq_context_socket_count_get(context) > 0) {
			php_error_docref(NULL, E_WARNING, "php_zmq_context_socket_count_get() > 0, will cause infinite hang on zmq_term, unset all zmqsocket references manually eg. via shutdown func");
		}
		(void) zmq_term(context->z_ctx);
	}
	pefree(context, context->is_persistent);
}
/* }}} */

/* {{{ static void php_zmq_socket_destroy(php_zmq_socket *zmq_sock)
	Destroy the socket (note: does not touch context)
*/
static void php_zmq_socket_destroy(php_zmq_socket *zmq_sock)
{
	zend_hash_destroy(&(zmq_sock->connect));
	zend_hash_destroy(&(zmq_sock->bind));

	/* Decrement socket count */
	php_zmq_context_socket_count_decr(zmq_sock->ctx);

	if (zmq_sock->pid == getpid ()) {
		(void) zmq_close(zmq_sock->z_socket);
	}
	pefree(zmq_sock, zmq_sock->is_persistent);
}
/* }}} */

/* --- START ZMQContext --- */

/* {{{ static php_zmq_context *php_zmq_context_new(zend_long io_threads, zend_bool is_persistent, zend_bool use_shared_ctx)
	Create a new zmq context
*/
static
php_zmq_context *php_zmq_context_new(zend_long io_threads, zend_bool is_persistent, zend_bool use_shared_ctx)
{
	php_zmq_context *context = pecalloc(1, sizeof(php_zmq_context), is_persistent);

	if (use_shared_ctx) {
		php_zmq_shared_ctx_assign_to(context);
	}
	else {
		context->z_ctx = zmq_init(io_threads);
	}

	if (!context->z_ctx) {
		pefree(context, is_persistent);
		return NULL;
	}

	context->io_threads     = io_threads;
	context->is_persistent  = is_persistent;
	context->pid            = getpid();
	context->socket_count   = 0;
	context->use_shared_ctx = use_shared_ctx;
	return context;
}
/* }}} */

/* {{{ static php_zmq_context *php_zmq_context_get(zend_long io_threads, zend_bool is_persistent)
*/
static
php_zmq_context *php_zmq_context_get(zend_long io_threads, zend_bool is_persistent)
{
	php_zmq_context *context;
	zend_string *plist_key = NULL;

	if (is_persistent) {
		zend_resource *le_p = NULL;
		plist_key = strpprintf(0, "zmq_context=[%ld]", (long)io_threads);

		if ((le_p = zend_hash_find_ptr(&EG(persistent_list), plist_key)) != NULL) {
			if (le_p->type == php_zmq_context_list_entry()) {

				if (plist_key) {
					zend_string_release(plist_key);
				}
				return (php_zmq_context *) le_p->ptr;
			}
		}
	}

	context = php_zmq_context_new(io_threads, is_persistent, 0);

	if (!context) {
		if (plist_key) {
			zend_string_release(plist_key);
		}
		return NULL;
	}

	if (is_persistent) {

		zend_resource le;
		le.type = php_zmq_context_list_entry();
		le.ptr  = context;

#if PHP_VERSION_ID < 70300
		GC_REFCOUNT(&le) = 1;
#else
		GC_SET_REFCOUNT(&le, 1);
#endif

		/* plist_key is not a persistent allocated key, thus we use str_update here */
		if (zend_hash_str_update_mem(&EG(persistent_list), plist_key->val, plist_key->len, &le, sizeof(le)) == NULL) {
			if (plist_key) {
				zend_string_release(plist_key);
			}
			php_error_docref(NULL, E_ERROR, "Could not register persistent entry for the context");
			/* not reached */
		}
	}
	if (plist_key) {
		zend_string_release(plist_key);
	}
	return context;
}
/* }}} */

/* {{{ proto ZMQ ZMQ::__construct()
	Private constructor
*/
PHP_METHOD(zmq, __construct) {}
/* }}} */

/* {{{ proto integer ZMQ::clock()
	A monotonic clock
*/
PHP_METHOD(zmq, clock)
{
	if (zend_parse_parameters_none() == FAILURE) {
		return;
	}
	RETURN_LONG((long) php_zmq_clock (ZMQ_G (clock_ctx)));
}
/* }}} */

#ifdef PHP_ZMQ_HAVE_Z85

/* {{{ proto string ZMQ::z85Encode(string $data)
	
*/
PHP_METHOD(zmq, z85encode)
{
	zend_string *data;
	char *buffer;
	size_t buffer_size;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "S", &data) == FAILURE) {
		return;
	}

	if (!data->len) {
		RETURN_NULL();
	}

	buffer_size = 5 * data->len / 4 + 1;
	buffer = emalloc(buffer_size);

	if (!zmq_z85_encode (buffer, (const uint8_t *) data->val, data->len)) {
		efree(buffer);
		RETURN_NULL();
	}

	RETVAL_STRINGL(buffer, buffer_size - 1);
	efree(buffer);
	return;
}
/* }}} */

/* {{{ proto string ZMQ::z85Decode(string $z85_encoded_data)
	
*/
PHP_METHOD(zmq, z85decode)
{
	zend_string *data;
	uint8_t *buffer;
	size_t buffer_size;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "S", &data) == FAILURE) {
		return;
	}

	if (!data->len) {
		RETURN_NULL();
	}

	buffer_size = 4 * data->len / 5;
	buffer = emalloc(buffer_size);

	if (!zmq_z85_decode (buffer, data->val)) {
		efree(buffer);
		RETURN_NULL();
	}

	RETVAL_STRINGL((char *) buffer, buffer_size);
	efree(buffer);
	return;
}
/* }}} */
#endif

#ifdef PHP_ZMQ_HAVE_CURVE_KEYPAIR

#define PHP_ZMQ_KEY_SIZE 41

/* {{{ proto array ZMQ::curveKeypair()
	
*/
PHP_METHOD(zmq, curvekeypair)
{
	char public_key[PHP_ZMQ_KEY_SIZE], secret_key[PHP_ZMQ_KEY_SIZE];

	if (zend_parse_parameters_none() == FAILURE) {
		return;
	}

	if (zmq_curve_keypair(public_key, secret_key) == 0) {
		array_init(return_value);

		add_assoc_stringl(return_value, "public_key", public_key, PHP_ZMQ_KEY_SIZE - 1);
		add_assoc_stringl(return_value, "secret_key", secret_key, PHP_ZMQ_KEY_SIZE - 1);
	}
}
/* }}} */

#endif


/* {{{ proto ZMQContext ZMQContext::__construct(integer $io_threads[, boolean $is_persistent = true])
	Build a new ZMQContext object
*/
PHP_METHOD(zmqcontext, __construct)
{
	php_zmq_context_object *intern;
	zend_long io_threads = 1;
	zend_bool is_persistent = 1;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "|lb", &io_threads, &is_persistent) == FAILURE) {
		return;
	}
	intern          = php_zmq_context_fetch_object(Z_OBJ_P(getThis()));
	intern->context = php_zmq_context_get(io_threads, is_persistent);

	if (!intern->context) {
		zend_throw_exception_ex(php_zmq_context_exception_sc_entry, errno, "Error creating context: %s", zmq_strerror(errno));
		return;
	}
	return;
}
/* }}} */

/* {{{ proto ZMQContext ZMQContext::acquire()
	Acquires a handle to the process global context
*/
PHP_METHOD(zmqcontext, acquire)
{
	php_zmq_context *context;
	php_zmq_context_object *intern;

	if (zend_parse_parameters_none() == FAILURE) {
		return;
	}

	context = php_zmq_context_new(1, 1, 1);

	// Create a global context
	object_init_ex(return_value, php_zmq_context_sc_entry);
	intern          = php_zmq_context_fetch_object(Z_OBJ_P(return_value));
	intern->context = context;
	return;
}
/* }}} */

/* {{{ proto integer ZMQContext::getSocketCount()
	Number of active sockets in this context
*/
PHP_METHOD(zmqcontext, getsocketcount)
{
	php_zmq_context_object *intern;

	if (zend_parse_parameters_none() == FAILURE) {
		return;
	}
	intern = PHP_ZMQ_CONTEXT_OBJECT;
	RETURN_LONG(php_zmq_context_socket_count_get(intern->context));
}
/* }}} */

#ifdef PHP_ZMQ_HAVE_CTX_OPTIONS
/* {{{ proto ZMQContext ZMQContext::setOpt(int option, int value)
	Set a context option
*/
PHP_METHOD(zmqcontext, setOpt)
{
	php_zmq_context_object *intern;
	zend_long option, value;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "ll", &option, &value) == FAILURE) {
		return;
	}
	intern = PHP_ZMQ_CONTEXT_OBJECT;

	switch (option) {
		case ZMQ_MAX_SOCKETS:
		{
			if (zmq_ctx_set(intern->context->z_ctx, option, value) != 0) {
				zend_throw_exception_ex(php_zmq_context_exception_sc_entry_get (), errno, "Failed to set the option ZMQ::CTXOPT_MAX_SOCKETS value: %s", zmq_strerror(errno));
				return;
			}
		}
		break;

		default:
		{
			zend_throw_exception(php_zmq_context_exception_sc_entry_get (), "Unknown option key", PHP_ZMQ_INTERNAL_ERROR);
			return;
		}
	}
	return;
}
/* }}} */

/* {{{ proto ZMQContext ZMQContext::getOpt(int option)
	Set a context option
*/
PHP_METHOD(zmqcontext, getOpt)
{
	php_zmq_context_object *intern;
	zend_long option;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "l", &option) == FAILURE) {
		return;
	}
	intern = PHP_ZMQ_CONTEXT_OBJECT;

	switch (option) {

		case ZMQ_MAX_SOCKETS:
		{
			int value = zmq_ctx_get(intern->context->z_ctx, option);
			RETURN_LONG(value);
		}
		break;

		default:
		{
			zend_throw_exception(php_zmq_context_exception_sc_entry_get (), "Unknown option key", PHP_ZMQ_INTERNAL_ERROR);
			return;
		}
	}
	return;
}
/* }}} */
#endif


/* {{{ static php_zmq_socket *php_zmq_socket_new(php_zmq_context *context, int type, zend_bool is_persistent)
	Create a new zmq socket
*/
static
php_zmq_socket *php_zmq_socket_new(php_zmq_context *context, zend_long type, zend_bool is_persistent)
{
	php_zmq_socket *zmq_sock;

	zmq_sock              = pecalloc(1, sizeof(php_zmq_socket), is_persistent);
	zmq_sock->z_socket    = zmq_socket(context->z_ctx, type);
	zmq_sock->pid         = getpid();
	zmq_sock->ctx         = context;
	zmq_sock->socket_type = type;

	if (!zmq_sock->z_socket) {
		pefree(zmq_sock, is_persistent);
		return NULL;
	}

	/* Increment socket count */
	php_zmq_context_socket_count_incr(context);
	zmq_sock->is_persistent = is_persistent;

	zend_hash_init(&(zmq_sock->connect), 0, NULL, NULL, is_persistent);
	zend_hash_init(&(zmq_sock->bind),    0, NULL, NULL, is_persistent);
	return zmq_sock;
}
/* }}} */

static
zend_string *php_zmq_socket_plist_key(zend_long type, zend_string *persistent_id, zend_bool use_shared_ctx)
{
	return strpprintf(0, "zmq_socket:[%ld]-[%s]-[%d]", (long)type, persistent_id->val, use_shared_ctx);
}

static
void php_zmq_socket_store(php_zmq_socket *zmq_sock_p, zend_long type, zend_string *persistent_id, zend_bool use_shared_ctx)
{
	zend_string *plist_key = NULL;

	zend_resource le;
	le.type = php_zmq_socket_list_entry();
	le.ptr  = zmq_sock_p;

#if PHP_VERSION_ID < 70300
	GC_REFCOUNT(&le) = 1;
#else
	GC_SET_REFCOUNT(&le, 1);
#endif

	plist_key = php_zmq_socket_plist_key(type, persistent_id, use_shared_ctx);

	/* plist_key is not a persistent allocated key, thus we use str_update here */
	if (zend_hash_str_update_mem(&EG(persistent_list), plist_key->val, plist_key->len, &le, sizeof(le)) == NULL) {
		if (plist_key) {
			zend_string_release(plist_key);
		}
		php_error_docref(NULL, E_ERROR, "Could not register persistent entry for the socket");
		/* not reached */
	}

	if (plist_key) {
		zend_string_release(plist_key);
	}
}

/* {{{ static php_zmq_socket *php_zmq_socket_get(php_zmq_context *context, zend_long type, zend_string *persistent_id, zend_bool *is_new)
	Tries to get context from plist and allocates a new context if context does not exist
*/
static php_zmq_socket *php_zmq_socket_get(php_zmq_context *context, zend_long type, zend_string *persistent_id, zend_bool *is_new)
{
	php_zmq_socket *zmq_sock_p;
	zend_string *plist_key = NULL;
	zend_bool is_persistent = 0;

	is_persistent = (context->is_persistent && persistent_id && persistent_id->len) ? 1 : 0;
	*is_new        = 0;

	if (is_persistent) {
		zend_resource *le_p = NULL;

		plist_key = php_zmq_socket_plist_key(type, persistent_id, context->use_shared_ctx);

		if ((le_p = zend_hash_find_ptr(&EG(persistent_list), plist_key)) != NULL) {
			if (le_p->type == php_zmq_socket_list_entry()) {
				if (plist_key) {
					zend_string_release(plist_key);
				}
				return (php_zmq_socket *) le_p->ptr;
			}
		}
	}
	zmq_sock_p = php_zmq_socket_new(context, type, is_persistent);

	if (plist_key) {
		zend_string_release(plist_key);
	}

	if (!zmq_sock_p) {
		return NULL;
	}

	*is_new = 1;
	return zmq_sock_p;
}
/* }}} */

static
zend_bool php_zmq_connect_callback(zval *socket, zend_fcall_info *fci, zend_fcall_info_cache *fci_cache, zend_string *persistent_id)
{
	zval retval;
	zval params[2];
	zend_bool rv = 1;

	/* Call the cb */
	params[0] = *socket;

	if (persistent_id && persistent_id->len) {
		ZVAL_STR(&params[1], zend_string_copy(persistent_id));
	} else {
		ZVAL_NULL(&params[1]);
	}

	fci->params         = params;
	fci->param_count    = 2;
	fci->retval         = &retval;
#if PHP_VERSION_ID < 80000
	fci->no_separation  = 1;
#endif

	if (zend_call_function(fci, fci_cache) == FAILURE) {
		if (!EG(exception)) {
			char *func_name = php_zmq_printable_func(fci, fci_cache);
			zend_throw_exception_ex(php_zmq_socket_exception_sc_entry, 0, "Failed to invoke 'on_new_socket' callback %s()", func_name);
			efree(func_name);
		}
		rv = 0;
	}

	zval_ptr_dtor(&params[1]);

	if (Z_TYPE(retval) != IS_UNDEF) {
		zval_ptr_dtor(&retval);
	}

	if (EG(exception)) {
		rv = 0;
	}
	return rv;
}

/* {{{ proto ZMQContext ZMQContext::getSocket(integer $type[, string $persistent_id = null[, callback $on_new_socket = null]])
	Build a new ZMQContext object
*/
PHP_METHOD(zmqcontext, getsocket)
{
	php_zmq_socket *socket;
	php_zmq_socket_object *interns;
	php_zmq_context_object *intern;
	zend_long type;
	zend_string *persistent_id = NULL;
	int rc;
	zend_bool is_new;

	zend_fcall_info fci;
	zend_fcall_info_cache fci_cache;

	PHP_ZMQ_ERROR_HANDLING_INIT()
	PHP_ZMQ_ERROR_HANDLING_THROW()

	fci.size = 0;
	rc = zend_parse_parameters(ZEND_NUM_ARGS(), "l|S!f!", &type, &persistent_id, &fci, &fci_cache);

	PHP_ZMQ_ERROR_HANDLING_RESTORE()

	if (rc == FAILURE) {
		return;
	}

	intern = php_zmq_context_fetch_object(Z_OBJ_P(getThis()));
	socket = php_zmq_socket_get(intern->context, type, persistent_id, &is_new);

	if (!socket) {
		zend_throw_exception_ex(php_zmq_socket_exception_sc_entry, errno, "Error creating socket: %s", zmq_strerror(errno));
		return;
	}

	object_init_ex(return_value, php_zmq_socket_sc_entry);
	interns         = php_zmq_socket_fetch_object(Z_OBJ_P(return_value));
	interns->socket = socket;

	/* Need to add refcount if context is not persistent */
	if (!intern->context->is_persistent) {
		ZVAL_OBJ(&interns->context_obj, &intern->zo);
		Z_ADDREF(interns->context_obj);
	}

	if (is_new) {
		if (fci.size) {
			if (!php_zmq_connect_callback(return_value, &fci, &fci_cache, persistent_id)) {
				php_zmq_socket_destroy(socket);
				interns->socket = NULL;
				return;
			}
		}
		if (socket->is_persistent) {
			php_zmq_socket_store(socket, type, persistent_id, intern->context->use_shared_ctx);
		}
	}
	if (socket->is_persistent) {
		interns->persistent_id = estrdup(persistent_id->val);
	}
	return;
}
/* }}} */

/* {{{ proto ZMQContext ZMQContext::isPersistent()
	Whether the context is persistent
*/
PHP_METHOD(zmqcontext, ispersistent)
{
	php_zmq_context_object *intern;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "") == FAILURE) {
		return;
	}

	intern = PHP_ZMQ_CONTEXT_OBJECT;
	RETURN_BOOL(intern->context->is_persistent);
}
/* }}} */

/* {{{ proto ZMQContext ZMQContext::__clone()
	Clones the instance of the ZMQContext class
*/
PHP_METHOD(zmqcontext, __clone) { }
/* }}} */

/* --- END ZMQContext --- */

/* --- START ZMQSocket --- */

/* {{{ proto ZMQSocket ZMQSocket::__construct(ZMQContext $context, integer $type[, string $persistent_id = null[, callback $on_new_socket = null]])
	Build a new ZMQSocket object
*/
PHP_METHOD(zmqsocket, __construct)
{
	php_zmq_socket *socket;
	php_zmq_socket_object *intern;
	php_zmq_context_object *internc;
	zend_long type;
	zend_string *persistent_id = NULL;
	int rc;
	zval *obj;
	zend_bool is_new;

	zend_fcall_info fci;
	zend_fcall_info_cache fci_cache;

	PHP_ZMQ_ERROR_HANDLING_INIT()
	PHP_ZMQ_ERROR_HANDLING_THROW()

	fci.size = 0;
	rc = zend_parse_parameters(ZEND_NUM_ARGS(), "Ol|S!f!", &obj, php_zmq_context_sc_entry, &type, &persistent_id, &fci, &fci_cache);

	PHP_ZMQ_ERROR_HANDLING_RESTORE()

	if (rc == FAILURE) {
		return;
	}

	internc = php_zmq_context_fetch_object(Z_OBJ_P(obj));
	socket  = php_zmq_socket_get(internc->context, type, persistent_id, &is_new);

	if (!socket) {
		zend_throw_exception_ex(php_zmq_socket_exception_sc_entry, errno, "Error creating socket: %s", zmq_strerror(errno));
		return;
	}

	intern         = PHP_ZMQ_SOCKET_OBJECT;
	intern->socket = socket;

	/* Need to add refcount if context is not persistent */
	if (!internc->context->is_persistent) {
		ZVAL_OBJ(&intern->context_obj, &internc->zo);
		Z_ADDREF(intern->context_obj);
	}

	if (is_new) {
		if (fci.size) {
			if (!php_zmq_connect_callback(getThis(), &fci, &fci_cache, persistent_id)) {
				php_zmq_socket_destroy(socket);
				intern->socket = NULL;
				return;
			}
		}
		if (socket->is_persistent) {
			php_zmq_socket_store(socket, type, persistent_id, internc->context->use_shared_ctx);
		}
	}
	if (socket->is_persistent) {
		intern->persistent_id = estrdup(persistent_id->val);
	}

	return;
}
/* }}} */

/* {{{ static zend_bool php_zmq_send(php_zmq_socket_object *intern, char *message_param, long flags)
*/
static zend_bool php_zmq_send(php_zmq_socket_object *intern, zend_string *message_param, zend_long flags)
{
	int rc, errno_;
	zmq_msg_t message;

	if (zmq_msg_init_size(&message, message_param->len) != 0) {
		zend_throw_exception_ex(php_zmq_socket_exception_sc_entry, errno, "Failed to initialize message structure: %s", zmq_strerror(errno));
		return 0;
	}
	memcpy(zmq_msg_data(&message), message_param->val, message_param->len);

	rc = zmq_sendmsg(intern->socket->z_socket, &message, flags);
	errno_ = errno;

	zmq_msg_close(&message);

	if (rc == -1) {
	    if (errno_ == EAGAIN) {
			return 0;
	    }
		zend_throw_exception_ex(php_zmq_socket_exception_sc_entry, errno_, "Failed to send message: %s", zmq_strerror(errno_));
		return 0;
	}

	return 1;
}
/* }}} */

static void php_zmq_sendmsg_impl(INTERNAL_FUNCTION_PARAMETERS)
{
	php_zmq_socket_object *intern;
	zend_string *message_param;
	zend_long flags = 0;
	zend_bool ret;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "S|l", &message_param, &flags) == FAILURE) {
		return;
	}
	intern = PHP_ZMQ_SOCKET_OBJECT;
	ret = php_zmq_send(intern, message_param, flags);

	if (ret) {
		ZMQ_RETURN_THIS;
	} else {
		RETURN_FALSE;
	}
}

/* {{{ proto ZMQSocket ZMQSocket::send(string $message[, integer $flags = 0])
	Send a message. Return true if message was sent and false on EAGAIN
*/
PHP_METHOD(zmqsocket, send)
{
	php_zmq_sendmsg_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}
/* }}} */


static
int php_zmq_send_cb(zval *zv, int num_args, va_list args, zend_hash_key *hash_key)
{
	php_zmq_socket_object *intern;
	int flags, *rc, *to_send;

	intern  = va_arg(args, php_zmq_socket_object *);
	flags   = va_arg(args, int);
	to_send = va_arg(args, int *);
	rc      = va_arg(args, int *);

	if (--(*to_send)) {
		flags = flags | ZMQ_SNDMORE;
	} else {
		flags = flags & ~ZMQ_SNDMORE;
	}

	zend_string *str = zval_get_string(zv);
	*rc = php_zmq_send(intern, str, flags);

	zend_string_release(str);

	if (!*rc) {
		return ZEND_HASH_APPLY_STOP;
	}
	return ZEND_HASH_APPLY_KEEP;
}

/* {{{ proto ZMQSocket ZMQSocket::sendmulti(arrays $messages[, integer $flags = 0])
	Send a multipart message. Return true if message was sent and false on EAGAIN
*/
PHP_METHOD(zmqsocket, sendmulti)
{
	zval *messages;
	php_zmq_socket_object *intern;
	int to_send, ret = 0;
	zend_long flags = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "a|l", &messages, &flags) == FAILURE) {
		return;
	}

	intern = PHP_ZMQ_SOCKET_OBJECT;
	to_send = zend_hash_num_elements(Z_ARRVAL_P(messages));
	zend_hash_apply_with_arguments(Z_ARRVAL_P(messages), (apply_func_args_t) php_zmq_send_cb, 4, intern, flags, &to_send, &ret);

	if (ret) {
		ZMQ_RETURN_THIS;
	} else {
		RETURN_FALSE;
	}
}

/* {{{ static zend_bool php_zmq_recv(php_zmq_socket_object *intern, long flags, zval *return_value)
*/
static
zend_string *php_zmq_recv(php_zmq_socket_object *intern, zend_long flags)
{
	int rc, errno_;
	zmq_msg_t message;
	zend_string *str = NULL;

	if (zmq_msg_init(&message) != 0) {
		zend_throw_exception_ex(php_zmq_socket_exception_sc_entry, errno, "Failed to initialize message structure: %s", zmq_strerror(errno));
		return NULL;
	}

	rc = zmq_recvmsg(intern->socket->z_socket, &message, flags);
	errno_ = errno;

	if (rc == -1) {
		zmq_msg_close(&message);
		if (errno == EAGAIN) {
			return NULL;
		}
		zend_throw_exception_ex(php_zmq_socket_exception_sc_entry, errno_, "Failed to receive message: %s", zmq_strerror(errno_));
		return NULL;
	}

	str = zend_string_init(zmq_msg_data(&message), zmq_msg_size(&message), 0);
	zmq_msg_close(&message);
	return str;
}
/* }}} */

static void php_zmq_recvmsg_impl(INTERNAL_FUNCTION_PARAMETERS)
{
	zend_string *str = NULL;
	php_zmq_socket_object *intern;
	zend_long flags = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "|l", &flags) == FAILURE) {
		return;
	}

	intern = PHP_ZMQ_SOCKET_OBJECT;
	str = php_zmq_recv(intern, flags);

	if (!str) {
		RETURN_FALSE;
	}
	RETURN_STR(str);
}

/* {{{ proto string ZMQ::recv([integer $flags = 0])
	Receive a message
*/
PHP_METHOD(zmqsocket, recv)
{
	php_zmq_recvmsg_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}
/* }}} */

/* {{{ proto array ZMQ::recvmulti([integer $flags = 0])
	Receive an array of message parts
*/
PHP_METHOD(zmqsocket, recvmulti)
{
	php_zmq_socket_object *intern;
	size_t value_len;
	zend_long flags = 0;
#if ZMQ_VERSION_MAJOR < 3
	int64_t value;
#else
	int value;
#endif

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "|l", &flags) == FAILURE) {
		return;
	}

	intern = PHP_ZMQ_SOCKET_OBJECT;
	array_init(return_value);
	value_len = sizeof (value);

	do {
		zend_string *part = php_zmq_recv(intern, flags);
		if (!part) {
			zval_dtor(return_value);
			RETURN_FALSE;
		}
		add_next_index_str(return_value, part);
		zmq_getsockopt(intern->socket->z_socket, ZMQ_RCVMORE, &value, &value_len);
	} while (value > 0);

	return;
}
/* }}} */

#if PHP_ZMQ_HAVE_SOCKET_MONITOR

/* {{{ proto array ZMQ::recvevent([integer $flags = 0])
	Receive an event from monitor endpoint
*/
PHP_METHOD(zmqsocket, recvevent)
{
	php_zmq_socket_object *intern;
	zend_long flags = 0;
	int value;
	uint8_t *data;
	uint16_t event;
	zend_string *metadata, *address;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "|l", &flags) == FAILURE) {
		return;
	}

	intern = PHP_ZMQ_SOCKET_OBJECT;

	metadata = php_zmq_recv(intern, flags);
	if (!metadata || metadata->len != sizeof (uint16_t) + sizeof (uint32_t)) {
		if (metadata) {
			zend_string_release(metadata);
		}
		zend_throw_exception_ex(php_zmq_socket_exception_sc_entry, errno, "Invalid monitor message received: %s", zmq_strerror(errno));
		return;
	}
	address = php_zmq_recv(intern, flags);
	if (!address) {
		zend_string_release(metadata);
		zend_throw_exception_ex(php_zmq_socket_exception_sc_entry, errno, "Invalid monitor message received: %s", zmq_strerror(errno));
		return;
	}

	data  = (uint8_t *) metadata->val;
	event = *(uint16_t *) (data);
	value = *(uint32_t *) (data + sizeof(uint16_t));

	array_init(return_value);

	add_assoc_long(return_value, "event", event);
	add_assoc_long(return_value, "value", value);
	add_assoc_str(return_value, "address", address);

	zend_string_release(metadata);
	return;
}
/* }}} */

/* {{{ proto ZMQSocket ZMQ::monitor(string $dsn[, integer $events = ZMQ::EVENTS_ALL])
	Add a monitor
*/
PHP_METHOD(zmqsocket, monitor)
{
	php_zmq_socket_object *intern;

	zend_string *dsn;
	zend_long events = ZMQ_EVENT_ALL;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "S|l", &dsn, &events) == FAILURE) {
		return;
	}

	intern = PHP_ZMQ_SOCKET_OBJECT;

	if (zmq_socket_monitor(intern->socket->z_socket, dsn->val, events) != 0) {
		zend_throw_exception_ex(php_zmq_socket_exception_sc_entry, errno, "Failed to add socket monitor: %s", zmq_strerror(errno));
		return;
	}
	ZMQ_RETURN_THIS;
}
/* }}} */

#endif

/** {{{ string ZMQ::getPersistentId() 
	Returns the persistent id of the object
*/
PHP_METHOD(zmqsocket, getpersistentid)
{
	php_zmq_socket_object *intern;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "") == FAILURE) {
		return;
	}

	intern = PHP_ZMQ_SOCKET_OBJECT;

	if (intern->socket->is_persistent && intern->persistent_id) {
		RETURN_STRING(intern->persistent_id);
	}
	RETURN_NULL();
}
/* }}} */

/* {{{ proto ZMQSocket ZMQSocket::bind(string $dsn[, boolean $force = false])
	Bind the socket to an endpoint
*/
PHP_METHOD(zmqsocket, bind)
{
	php_zmq_socket_object *intern;
	zend_string *dsn;
	zend_bool force = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "S|b", &dsn, &force) == FAILURE) {
		return;
	}

	intern = php_zmq_socket_fetch_object(Z_OBJ_P(getThis()));

	/* already connected ? */
	if (!force && zend_hash_exists(&(intern->socket->bind), dsn)) {
		ZMQ_RETURN_THIS;
	}

	if (zmq_bind(intern->socket->z_socket, dsn->val) != 0) {
		zend_throw_exception_ex(php_zmq_socket_exception_sc_entry, errno, "Failed to bind the ZMQ: %s", zmq_strerror(errno));
		return;
	}

	zend_hash_str_add_empty_element(&(intern->socket->bind), dsn->val, dsn->len);
	ZMQ_RETURN_THIS;
}
/* }}} */

/* {{{ proto ZMQSocket ZMQSocket::connect(string $dsn[, boolean $force = false])
	Connect the socket to an endpoint
*/
PHP_METHOD(zmqsocket, connect)
{
	php_zmq_socket_object *intern;
	zend_string *dsn;
	zend_bool force = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "S|b", &dsn, &force) == FAILURE) {
		return;
	}

	intern = PHP_ZMQ_SOCKET_OBJECT;

	/* already connected ? */
	if (!force && zend_hash_exists(&(intern->socket->connect), dsn)) {
		ZMQ_RETURN_THIS;
	}

	if (zmq_connect(intern->socket->z_socket, dsn->val) != 0) {
		zend_throw_exception_ex(php_zmq_socket_exception_sc_entry, errno, "Failed to connect the ZMQ: %s", zmq_strerror(errno));
		return;
	}

	zend_hash_str_add_empty_element(&(intern->socket->connect), dsn->val, dsn->len);
	ZMQ_RETURN_THIS;
}
/* }}} */

#ifdef PHP_ZMQ_HAVE_UNBIND
/* {{{ proto ZMQSocket ZMQSocket::unbind(string $dsn)
	Unbind the socket from an endpoint
*/
PHP_METHOD(zmqsocket, unbind)
{
	php_zmq_socket_object *intern;
	zend_string *dsn;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "S", &dsn) == FAILURE) {
		return;
	}

	intern = PHP_ZMQ_SOCKET_OBJECT;

	if (zmq_unbind(intern->socket->z_socket, dsn->val) != 0) {
		zend_throw_exception_ex(php_zmq_socket_exception_sc_entry, errno, "Failed to unbind the ZMQ socket: %s", zmq_strerror(errno));
		return;
	}

	zend_hash_del(&(intern->socket->bind), dsn);
	ZMQ_RETURN_THIS;
}
/* }}} */
#endif

#ifdef PHP_ZMQ_HAVE_DISCONNECT
/* {{{ proto ZMQSocket ZMQSocket::disconnect(string $dsn)
	Disconnect the socket from an endpoint
*/
PHP_METHOD(zmqsocket, disconnect)
{
	php_zmq_socket_object *intern;
	zend_string *dsn;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "S", &dsn) == FAILURE) {
		return;
	}

	intern = PHP_ZMQ_SOCKET_OBJECT;

	if (zmq_disconnect(intern->socket->z_socket, dsn->val) != 0) {
		zend_throw_exception_ex(php_zmq_socket_exception_sc_entry, errno, "Failed to disconnect the ZMQ socket: %s", zmq_strerror(errno));
		return;
	}

	zend_hash_del(&(intern->socket->connect), dsn);
	ZMQ_RETURN_THIS;
}
/* }}} */
#endif


static
int php_zmq_get_keys(zval *ppzval, int num_args, va_list args, zend_hash_key *hash_key)
{
	zval *retval = va_arg(args, zval *);
	add_next_index_string(retval, hash_key->key->val);
	return ZEND_HASH_APPLY_KEEP;
}
/* }}} */

/* {{{ proto array ZMQ::getEndpoints()
	Returns endpoints where this socket is connected/bound to. Contains two keys ('bind', 'connect')
*/
PHP_METHOD(zmqsocket, getendpoints)
{
	php_zmq_socket_object *intern;
	zval connect, bind;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "") == FAILURE) {
		return;
	}

	intern = PHP_ZMQ_SOCKET_OBJECT;
	array_init(return_value);

	array_init(&connect);
	array_init(&bind);

	zend_hash_apply_with_arguments(&(intern->socket->connect), (apply_func_args_t) php_zmq_get_keys, 1, &connect);
	zend_hash_apply_with_arguments(&(intern->socket->bind), (apply_func_args_t) php_zmq_get_keys, 1, &bind);

	add_assoc_zval(return_value, "connect", &connect);
	add_assoc_zval(return_value, "bind", &bind);
	return;
}
/* }}} */

/* {{{ proto integer ZMQSocket::getSocketType()
	Returns the socket type
*/
PHP_METHOD(zmqsocket, getsockettype)
{
	int type;
	size_t type_siz;
	php_zmq_socket_object *intern;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "") == FAILURE) {
		return;
	}
	intern = PHP_ZMQ_SOCKET_OBJECT;
	type_siz = sizeof (int);

	if (zmq_getsockopt(intern->socket->z_socket, ZMQ_TYPE, &type, &type_siz) != -1) {
		RETURN_LONG(type);
	}
	RETURN_LONG(-1);
}
/* }}} */

/* {{{ proto boolean ZMQSocket::isPersistent()
	Whether the socket is persistent
*/
PHP_METHOD(zmqsocket, ispersistent)
{
	php_zmq_socket_object *intern;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "") == FAILURE) {
		return;
	}

	intern = PHP_ZMQ_SOCKET_OBJECT;
	RETURN_BOOL(intern->socket->is_persistent);
}
/* }}} */

/* {{{ proto ZMQSocket ZMQSocket::__clone()
	Clones the instance of the ZMQSocket class
*/
PHP_METHOD(zmqsocket, __clone) { }
/* }}} */

/* -- END ZMQSocket--- */

/* -- START ZMQPoll --- */

/* {{{ proto integer ZMQPoll::add(ZMQSocket $object, integer $events)
	Add a ZMQSocket object into the pollset
*/
PHP_METHOD(zmqpoll, add)
{
	php_zmq_poll_object *intern;
	zval *object;
	zend_long events;
	int error;
	zend_string *key;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "zl", &object, &events) == FAILURE) {
		return;
	}

	intern = PHP_ZMQ_POLL_OBJECT;

	switch (Z_TYPE_P(object)) {
		case IS_OBJECT:
			if (!instanceof_function(Z_OBJCE_P(object), php_zmq_socket_sc_entry)) {
				zend_throw_exception(php_zmq_poll_exception_sc_entry, "The first argument must be an instance of ZMQSocket or a resource", PHP_ZMQ_INTERNAL_ERROR);
				return;
			}
		break;

		case IS_RESOURCE:
			/* noop */
		break;

		default:
			zend_throw_exception(php_zmq_poll_exception_sc_entry, "The first argument must be an instance of ZMQSocket or a resource", PHP_ZMQ_INTERNAL_ERROR);
			return;
		break;
	}

	key = php_zmq_pollset_add(intern->set, object, events, &error);

	if (!key) {
		const char *message = NULL;

		switch (error) {
			case PHP_ZMQ_POLLSET_ERR_NO_STREAM:
				message = "The supplied resource is not a valid stream resource";
			break;

			case PHP_ZMQ_POLLSET_ERR_CANNOT_CAST:
				message = "The supplied resource is not castable";
			break;

			case PHP_ZMQ_POLLSET_ERR_CAST_FAILED:
				message = "Failed to cast the supplied stream resource";
			break;

			case PHP_ZMQ_POLLSET_ERR_NO_INIT:
				message = "The ZMQSocket object has not been initialized properly";
			break;

			case PHP_ZMQ_POLLSET_ERR_NO_POLL:
				message = "The ZMQSocket object has not been initialized with polling";
			break;

			default:
				message = "Unknown error";
			break;
		}

		zend_throw_exception(php_zmq_poll_exception_sc_entry, message, PHP_ZMQ_INTERNAL_ERROR);
		return;
	}
	RETURN_STR(key);
}
/* }}} */

/* {{{ proto boolean ZMQPoll::remove(mixed $item)
	Remove item from poll set
*/
PHP_METHOD(zmqpoll, remove)
{
	php_zmq_poll_object *intern;
	zval *item;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "z", &item) == FAILURE) {
		return;
	}

	intern = PHP_ZMQ_POLL_OBJECT;

	if (php_zmq_pollset_num_items(intern->set) == 0) {
		zend_throw_exception(php_zmq_poll_exception_sc_entry, "No sockets assigned to the ZMQPoll", PHP_ZMQ_INTERNAL_ERROR);
		return;
	}

	switch (Z_TYPE_P(item)) {

		case IS_OBJECT:
			if (!instanceof_function(Z_OBJCE_P(item), php_zmq_socket_sc_entry)) {
				zend_throw_exception(php_zmq_poll_exception_sc_entry, "The object must be an instanceof ZMQSocket", PHP_ZMQ_INTERNAL_ERROR);
				return;
			}
			/* break intentionally missing */
		case IS_RESOURCE:
			RETVAL_BOOL(php_zmq_pollset_delete(intern->set, item));
		break;

		default: {
			zend_string *str = zval_get_string(item);
			zend_bool retval;

			retval = php_zmq_pollset_delete_by_key(intern->set, str);
			zend_string_release(str);

			RETVAL_BOOL(retval);
		} break;
	}

	return;
}
/* }}} */

/* {{{ proto integer ZMQPoll::poll(array &$readable, array &$writable[, integer $timeout = -1])
	Poll the sockets
*/
PHP_METHOD(zmqpoll, poll)
{
	php_zmq_poll_object *intern;
	zval *r_array, *w_array;

	zend_long timeout = -1;
	int rc;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "a!/a!/|l", &r_array, &w_array, &timeout) == FAILURE) {
		return;
	}

	intern = PHP_ZMQ_POLL_OBJECT;

	if (php_zmq_pollset_num_items(intern->set) == 0) {
		zend_throw_exception(php_zmq_poll_exception_sc_entry, "No sockets assigned to the ZMQPoll", PHP_ZMQ_INTERNAL_ERROR);
		return;
	}

	rc = php_zmq_pollset_poll(intern->set, timeout * PHP_ZMQ_TIMEOUT, r_array, w_array);

	if (rc == -1) {
		zend_throw_exception_ex(php_zmq_poll_exception_sc_entry, errno, "Poll failed: %s", zmq_strerror(errno));
		return;
	}
	RETURN_LONG(rc);
}
/* }}} */

/* {{{ proto integer ZMQPoll::count()
	Returns the number of items in the set
*/
PHP_METHOD(zmqpoll, count)
{
	php_zmq_poll_object *intern;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "") == FAILURE) {
		return;
	}

	intern = PHP_ZMQ_POLL_OBJECT;
	RETURN_LONG(php_zmq_pollset_num_items(intern->set));
}
/* }}} */

/* {{{ proto ZMQPoll ZMQPoll::clear()
	Clear the pollset
*/
PHP_METHOD(zmqpoll, clear)
{
	php_zmq_poll_object *intern;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "") == FAILURE) {
		return;
	}

	intern = PHP_ZMQ_POLL_OBJECT;

	php_zmq_pollset_clear(intern->set);
	ZMQ_RETURN_THIS;
}
/* }}} */

/* {{{ proto array ZMQPoll::items()
	Clear the pollset
*/
PHP_METHOD(zmqpoll, items)
{
	php_zmq_poll_object *intern;

	if (zend_parse_parameters_none() == FAILURE) {
		return;
	}

	intern = PHP_ZMQ_POLL_OBJECT;

	array_init(return_value);
	php_zmq_pollset_items(intern->set, return_value);
}
/* }}} */

/* {{{ proto array ZMQPoll::getLastErrors()
	Returns last errors
*/
PHP_METHOD(zmqpoll, getlasterrors)
{
	php_zmq_poll_object *intern;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "") == FAILURE) {
		return;
	}

	intern = PHP_ZMQ_POLL_OBJECT;
	RETVAL_ZVAL(php_zmq_pollset_errors(intern->set), 1, 0);
	return;
}
/* }}} */

/* {{{ proto ZMQPoll ZMQPoll::__clone()
	Clones the instance of the ZMQPoll class
*/
PHP_METHOD(zmqpoll, __clone) { }
/* }}} */

/* -- END ZMQPoll */

/* {{{ proto void ZMQDevice::__construct(ZMQSocket frontend, ZMQSocket backend)
	Construct a device
*/
PHP_METHOD(zmqdevice, __construct)
{
	php_zmq_device_object *intern;
	zval *f, *b, *c = NULL;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "OO|O!", &f, php_zmq_socket_sc_entry, &b, php_zmq_socket_sc_entry, &c, php_zmq_socket_sc_entry) == FAILURE) {
		return;
	}

	intern = PHP_ZMQ_DEVICE_OBJECT;

	ZVAL_OBJ(&intern->front, Z_OBJ_P(f));
	Z_ADDREF(intern->front);

	ZVAL_OBJ(&intern->back, Z_OBJ_P(b));
	Z_ADDREF(intern->back);

	if (c) {
		ZVAL_OBJ(&intern->capture, Z_OBJ_P(c));
		Z_ADDREF(intern->capture);
	} else {
		ZVAL_UNDEF(&intern->capture);
	}
}
/* }}} */

/* {{{ proto void ZMQDevice::run()
	Start a device
*/
PHP_METHOD(zmqdevice, run)
{
	php_zmq_device_object *intern;
	zend_bool rc;

	if (zend_parse_parameters_none() == FAILURE) {
		return;
	}

	intern = PHP_ZMQ_DEVICE_OBJECT;
	rc = php_zmq_device (intern);

	if (!rc && !EG (exception)) {
		zend_throw_exception_ex(php_zmq_device_exception_sc_entry, errno, "Failed to start the device: %s", zmq_strerror (errno));
		return;
	}
	return;
}
/* }}} */

static
void s_clear_device_callback (php_zmq_device_cb_t *cb)
{
	if (cb->initialized) {
		zval_ptr_dtor(&cb->fci.function_name);
		cb->fci.size = 0;

		if (!Z_ISUNDEF(cb->user_data)) {
			zval_ptr_dtor(&cb->user_data);
		}
		memset (cb, 0, sizeof (php_zmq_device_cb_t));
		cb->initialized = 0;
	}
}

static
void s_init_device_callback (php_zmq_device_cb_t *cb, zend_fcall_info *fci, zend_fcall_info_cache *fci_cache, zend_long timeout, zval *user_data)
{
	memcpy (&cb->fci, fci, sizeof (zend_fcall_info));
	memcpy (&cb->fci_cache, fci_cache, sizeof (zend_fcall_info_cache));

	Z_TRY_ADDREF (fci->function_name);
	cb->initialized  = 1;
	cb->scheduled_at = php_zmq_clock (ZMQ_G (clock_ctx)) + timeout;
	cb->timeout      = timeout;

	if (user_data) {
		ZVAL_COPY(&cb->user_data, user_data);
	} else {
		ZVAL_NULL(&cb->user_data);
	}
}

/* {{{ proto void ZMQDevice::setIdleTimeout (int $milliseconds)
	Set the idle timeout value
*/
PHP_METHOD(zmqdevice, setidletimeout)
{
	php_zmq_device_object *intern;
	zend_long timeout;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "l", &timeout) == FAILURE) {
		return;
	}

	intern = PHP_ZMQ_DEVICE_OBJECT;
	intern->idle_cb.timeout = timeout;
	ZMQ_RETURN_THIS;

}
/* }}} */

PHP_METHOD(zmqdevice, getidletimeout)
{
	php_zmq_device_object *intern;

	if (zend_parse_parameters_none() == FAILURE) {
		return;
	}

	intern = PHP_ZMQ_DEVICE_OBJECT;
	RETURN_LONG(intern->idle_cb.timeout);
}


PHP_METHOD(zmqdevice, settimertimeout)
{
	php_zmq_device_object *intern;
	zend_long timeout;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "l", &timeout) == FAILURE) {
		return;
	}

	intern = PHP_ZMQ_DEVICE_OBJECT;
	intern->timer_cb.timeout = timeout;
	ZMQ_RETURN_THIS;
}

PHP_METHOD(zmqdevice, gettimertimeout)
{
	php_zmq_device_object *intern;

	if (zend_parse_parameters_none() == FAILURE) {
		return;
	}

	intern = PHP_ZMQ_DEVICE_OBJECT;
	RETURN_LONG(intern->timer_cb.timeout);
}

/* {{{ proto void ZMQDevice::setIdleCallback (callable $function, integer timeout [, mixed $userdata])
	Set the idle timeout value
*/
PHP_METHOD(zmqdevice, setidlecallback)
{
	php_zmq_device_object *intern;
	zval *user_data = NULL;
	zend_fcall_info fci;
	zend_fcall_info_cache fci_cache;
	zend_long timeout = 0;

	if (ZEND_NUM_ARGS() == 2) {
		php_error_docref(NULL, E_DEPRECATED, "The signature for setIdleCallback has changed, please update your code");
		if (zend_parse_parameters(ZEND_NUM_ARGS(), "f|z!", &fci, &fci_cache, &user_data) == FAILURE) {
			return;
		}
	}
	else {
		if (zend_parse_parameters(ZEND_NUM_ARGS(), "fl|z!", &fci, &fci_cache, &timeout, &user_data) == FAILURE) {
			return;
		}
	}

	intern = PHP_ZMQ_DEVICE_OBJECT;

	/* Hack for backwards compatible behaviour */
	if (!timeout) {
		if (intern->idle_cb.timeout) {
			timeout = intern->idle_cb.timeout;
		}
	}

	s_clear_device_callback (&intern->idle_cb);

	if (fci.size > 0) {
		s_init_device_callback (&intern->idle_cb, &fci, &fci_cache, timeout, user_data);
	}
	ZMQ_RETURN_THIS;

}
/* }}} */

/* {{{ proto void ZMQDevice::setTimerCallback (callable $function, integer timeout [, mixed $userdata])
	Set the timer function
*/
PHP_METHOD(zmqdevice, settimercallback)
{
	php_zmq_device_object *intern;
	zval *user_data = NULL;
	zend_fcall_info fci;
	zend_fcall_info_cache fci_cache;
	zend_long timeout;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "fl|z!", &fci, &fci_cache, &timeout, &user_data) == FAILURE) {
		return;
	}

	intern = PHP_ZMQ_DEVICE_OBJECT;

	s_clear_device_callback (&intern->timer_cb);
	if (fci.size > 0) {
		s_init_device_callback (&intern->timer_cb, &fci, &fci_cache, timeout, user_data);
	}
	ZMQ_RETURN_THIS;
}
/* }}} */

/* {{{ proto ZMQDevice ZMQDevice::__clone()
	Clones the instance of the ZMQDevice class
*/
PHP_METHOD(zmqdevice, __clone) { }
/* }}} */

/* -- END ZMQPoll */

#ifdef HAVE_CZMQ_2

/* --- START q --- */

static
void php_zmq_cert_object_free_storage(zend_object *object)
{
	php_zmq_cert_object *intern = php_zmq_cert_fetch_object(object);

	if (!intern) {
		return;
	}

	if (intern->zcert) {
		zcert_destroy(&intern->zcert);
	}
	zend_object_std_dtor(&intern->zo);
}

static
zend_object *php_zmq_cert_new(zend_class_entry *class_type)
{
	php_zmq_cert_object *intern = ecalloc(1, sizeof(php_zmq_cert_object) + zend_object_properties_size(class_type));

	/* zcert is initialised in ZMQCert#__construct. */
	intern->zcert = NULL;

	zend_object_std_init(&intern->zo, class_type);
	object_properties_init(&intern->zo, class_type);

	intern->zo.handlers = &zmq_cert_object_handlers;
	return &intern->zo;
}

PHP_METHOD(zmqcert, __construct)
{
	zend_string *filename = NULL;
	zend_error_handling error_handling;
	int parse_parameters_result;
	php_zmq_cert_object *intern;

	zend_replace_error_handling(EH_THROW, php_zmq_cert_exception_sc_entry, &error_handling);
	parse_parameters_result = zend_parse_parameters(ZEND_NUM_ARGS(), "|S", &filename);
	zend_restore_error_handling(&error_handling);

	if (parse_parameters_result != SUCCESS) {
		return;
	}

	intern = PHP_ZMQ_CERT_OBJECT;

	if (!filename) {
		intern->zcert = zcert_new();

		if (!intern->zcert) {
			zend_throw_exception_ex(php_zmq_cert_exception_sc_entry, PHP_ZMQ_INTERNAL_ERROR, "Failed to create the underlying zcert object. Is libsodium installed?", filename);
		}
		return;
	}

	intern->zcert = zcert_load(filename->val);

	if (!intern->zcert) {
		zend_throw_exception_ex(php_zmq_cert_exception_sc_entry, PHP_ZMQ_INTERNAL_ERROR, "Failed to load the certificate from %s", filename->val);
	}

	return;
}

ZEND_BEGIN_ARG_INFO_EX(zmqcert___construct_args, 0, 0, 0)
	ZEND_ARG_INFO(0, filename)
ZEND_END_ARG_INFO();

PHP_METHOD(zmqcert, getPublicKey)
{
	php_zmq_cert_object *intern;
	byte *public_key;

	if (zend_parse_parameters_none() != SUCCESS) {
		return;
	}

	intern = PHP_ZMQ_CERT_OBJECT;
	public_key = zcert_public_key(intern->zcert);

	if (!public_key) {
		RETURN_NULL();
	}
	RETURN_STRINGL((char *) public_key, 32);
}

ZEND_BEGIN_ARG_INFO_EX(zmqcert_getPublicKey_args, 0, 0, 0)
ZEND_END_ARG_INFO()

PHP_METHOD(zmqcert, getSecretKey)
{
	php_zmq_cert_object *intern;
	byte *secret_key;

	if (zend_parse_parameters_none() != SUCCESS) {
		return;
	}

	intern = PHP_ZMQ_CERT_OBJECT;
	secret_key = zcert_secret_key(intern->zcert);

	if (!secret_key) {
		RETURN_NULL();
	}
	RETURN_STRINGL((char *) secret_key, 32);
}

ZEND_BEGIN_ARG_INFO_EX(zmqcert_getSecretKey_args, 0, 0, 0)
ZEND_END_ARG_INFO()

PHP_METHOD(zmqcert, getPublicTxt)
{
	php_zmq_cert_object *intern;
	char *public_txt;

	if (zend_parse_parameters_none() != SUCCESS) {
		return;
	}

	intern = PHP_ZMQ_CERT_OBJECT;
	public_txt = zcert_public_txt(intern->zcert);

	if (!public_txt) {
		RETURN_NULL();
	}
	RETURN_STRING(public_txt);
}

ZEND_BEGIN_ARG_INFO_EX(zmqcert_getPublicTxt_args, 0, 0, 0)
ZEND_END_ARG_INFO()

PHP_METHOD(zmqcert, getSecretTxt)
{
	php_zmq_cert_object *intern;
	char *secret_txt;

	if (zend_parse_parameters_none() != SUCCESS) {
		return;
	}

	intern = PHP_ZMQ_CERT_OBJECT;
	secret_txt = zcert_secret_txt(intern->zcert);

	if (!secret_txt) {
		RETURN_NULL();
	}
	RETURN_STRING(secret_txt);
}

ZEND_BEGIN_ARG_INFO_EX(zmqcert_getSecretTxt_args, 0, 0, 0)
ZEND_END_ARG_INFO()

PHP_METHOD(zmqcert, setMeta)
{
	php_zmq_cert_object *intern;
	zend_string *name;
	zend_string *format;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "SS", &name, &format) != SUCCESS) {
		return;
	}

	intern = PHP_ZMQ_CERT_OBJECT;
	zcert_set_meta(intern->zcert, name->val, format->val);

	return;
}

ZEND_BEGIN_ARG_INFO_EX(zmqcert_setMeta_args, 0, 0, 2)
	ZEND_ARG_INFO(0, name)
	ZEND_ARG_INFO(0, format)
ZEND_END_ARG_INFO()

PHP_METHOD(zmqcert, getMeta)
{
	zend_string *name;
	php_zmq_cert_object *intern;
	char *result;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "S", &name) != SUCCESS) {
		return;
	}

	intern = PHP_ZMQ_CERT_OBJECT;
	result = zcert_meta(intern->zcert, name->val);

	if (!result) {
		RETURN_NULL();
	}
	RETURN_STRING(result);
}

ZEND_BEGIN_ARG_INFO_EX(zmqcert_getMeta_args, 0, 0, 1)
	ZEND_ARG_INFO(0, name)
ZEND_END_ARG_INFO()

PHP_METHOD(zmqcert, getMetaKeys)
{
	php_zmq_cert_object *intern;
	zlist_t *meta_keys;
	char *meta_key;

	if (zend_parse_parameters_none() != SUCCESS) {
		return;
	}

	intern = PHP_ZMQ_CERT_OBJECT;

	meta_keys = zcert_meta_keys(intern->zcert);
	meta_key = (char *) zlist_first(meta_keys);

	array_init(return_value);
	while (meta_key) {
		add_next_index_string(return_value, meta_key);
		meta_key = zlist_next(meta_keys);
	}

	return;
}

ZEND_BEGIN_ARG_INFO_EX(zmqcert_getMetaKeys_args, 0, 0, 0)
ZEND_END_ARG_INFO()

PHP_METHOD(zmqcert, save)
{
	php_zmq_cert_object *intern;
	zend_string *filename;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "S", &filename) != SUCCESS) {
		return;
	}

	intern = PHP_ZMQ_CERT_OBJECT;

	if (zcert_save(intern->zcert, filename->val) == -1) {
		zend_throw_exception_ex(php_zmq_cert_exception_sc_entry, PHP_ZMQ_INTERNAL_ERROR, "Failed to save the certificate to %s", filename->val);
	}

	return;
}

ZEND_BEGIN_ARG_INFO_EX(zmqcert_save_args, 0, 0, 1)
	ZEND_ARG_INFO(0, filename)
ZEND_END_ARG_INFO()

PHP_METHOD(zmqcert, savePublic)
{
	zend_string *filename;
	php_zmq_cert_object *intern;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "S", &filename) != SUCCESS) {
		return;
	}

	intern = PHP_ZMQ_CERT_OBJECT;

	if (zcert_save_public(intern->zcert, filename->val) == -1) {
		zend_throw_exception_ex(php_zmq_cert_exception_sc_entry, PHP_ZMQ_INTERNAL_ERROR, "Failed to save the public certificate to %s", filename->val);
	}

	return;
}

ZEND_BEGIN_ARG_INFO_EX(zmqcert_savePublic_args, 0, 0, 1)
	ZEND_ARG_INFO(0, filename)
ZEND_END_ARG_INFO()

PHP_METHOD(zmqcert, saveSecret)
{
	zend_string *filename;
	php_zmq_cert_object *intern;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "S", &filename) != SUCCESS) {
		return;
	}

	intern = PHP_ZMQ_CERT_OBJECT;

	if (zcert_save_secret(intern->zcert, filename->val) == -1) {
		zend_throw_exception_ex(php_zmq_cert_exception_sc_entry, PHP_ZMQ_INTERNAL_ERROR, "Failed to save the secret certificate to %s", filename->val);
	}

	return;
}

ZEND_BEGIN_ARG_INFO_EX(zmqcert_saveSecret_args, 0, 0, 1)
	ZEND_ARG_INFO(0, filename)
ZEND_END_ARG_INFO()

PHP_METHOD(zmqcert, apply)
{
	php_zmq_cert_object *intern;
	zval *object;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "O", &object, php_zmq_socket_sc_entry) != SUCCESS) {
		return;
	}

	intern = PHP_ZMQ_CERT_OBJECT;
	zcert_apply(intern->zcert, php_zmq_socket_fetch_object(Z_OBJ_P(object))->socket->z_socket);
}

ZEND_BEGIN_ARG_INFO_EX(zmqcert_apply_args, 0, 0, 1)
	ZEND_ARG_OBJ_INFO(0, ZMQSocket, ZMQSocket, 0)
ZEND_END_ARG_INFO()

static
zend_object *php_zmq_cert_clone(zval *object)
{
	zend_object *result;
	php_zmq_cert_object *intern;
	php_zmq_cert_object *that;

	intern = php_zmq_cert_fetch_object(Z_OBJ_P(object));
	result = php_zmq_cert_new(php_zmq_cert_sc_entry);

	that = php_zmq_cert_fetch_object(result);
	that->zcert = zcert_dup(intern->zcert);
	return result;
}

PHP_METHOD(zmqcert, equals)
{
	php_zmq_cert_object *intern;
	zval *object;
	php_zmq_cert_object *that;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "O", &object, php_zmq_cert_sc_entry) != SUCCESS) {
		return;
	}

	intern = PHP_ZMQ_CERT_OBJECT;
	that = php_zmq_cert_fetch_object(Z_OBJ_P(object));

	RETURN_BOOL(zcert_eq(intern->zcert, that->zcert) == 1);
}

ZEND_BEGIN_ARG_INFO_EX(zmqcert_equals_args, 0, 0, 1)
	ZEND_ARG_OBJ_INFO(0, ZMQCert, ZMQCert, 0)
ZEND_END_ARG_INFO()

static zend_function_entry php_zmq_cert_class_methods[] = {
	PHP_ME(zmqcert, __construct,	zmqcert___construct_args,	ZEND_ACC_PUBLIC|ZEND_ACC_CTOR|ZEND_ACC_FINAL)
	PHP_ME(zmqcert, getPublicKey,	zmqcert_getPublicKey_args,	ZEND_ACC_PUBLIC)
	PHP_ME(zmqcert, getSecretKey,	zmqcert_getSecretKey_args,	ZEND_ACC_PUBLIC)
	PHP_ME(zmqcert, getPublicTxt,	zmqcert_getPublicTxt_args,	ZEND_ACC_PUBLIC)
	PHP_ME(zmqcert, getSecretTxt,	zmqcert_getSecretTxt_args,	ZEND_ACC_PUBLIC)
	PHP_ME(zmqcert, setMeta,		zmqcert_setMeta_args,		ZEND_ACC_PUBLIC)
	PHP_ME(zmqcert, getMeta,		zmqcert_getMeta_args,		ZEND_ACC_PUBLIC)
	PHP_ME(zmqcert, getMetaKeys,	zmqcert_getMetaKeys_args,	ZEND_ACC_PUBLIC)
	PHP_ME(zmqcert, save,			zmqcert_save_args,			ZEND_ACC_PUBLIC)
	PHP_ME(zmqcert, savePublic,		zmqcert_savePublic_args,	ZEND_ACC_PUBLIC)
	PHP_ME(zmqcert, saveSecret,		zmqcert_saveSecret_args,	ZEND_ACC_PUBLIC)
	PHP_ME(zmqcert, apply,			zmqcert_apply_args,			ZEND_ACC_PUBLIC)
	/* PHP_ME(zmqcert, __clone, zmqcert___clone_args, ZEND_ACC_PUBLIC) */
	PHP_ME(zmqcert, equals,			zmqcert_equals_args,		ZEND_ACC_PUBLIC)
	{NULL, NULL, NULL}
};

/* --- END ZMQCert --- */

/* --- START ZMQAuth --- */

static
void php_zmq_auth_object_free_storage(zend_object *object)
{
	php_zmq_auth_object *intern = php_zmq_auth_fetch_object(object);

	if (!intern) {
		return;
	}

	if (intern->zauth) {
		zauth_destroy(&intern->zauth);
	}
	if (intern->shadow_context) {
		zctx_destroy(&intern->shadow_context);
	}
	zend_object_std_dtor(&intern->zo);
}

static
zend_object *php_zmq_auth_new(zend_class_entry *class_type)
{
	php_zmq_auth_object *intern = ecalloc(1, sizeof(php_zmq_auth_object) + zend_object_properties_size(class_type));

	/* zcert is initialised in ZMQCert#__construct. */
	intern->zauth = NULL;

	zend_object_std_init(&intern->zo, class_type);
	object_properties_init(&intern->zo, class_type);

	intern->zo.handlers = &zmq_auth_object_handlers;
	return &intern->zo;
}

PHP_METHOD(zmqauth, __construct)
{
	php_zmq_auth_object *intern;
	zval *object;
	zend_error_handling error_handling;
	int parse_parameters_result;
	php_zmq_context_object *context_object;

	zend_replace_error_handling(EH_THROW, php_zmq_cert_exception_sc_entry, &error_handling);
	parse_parameters_result = zend_parse_parameters(ZEND_NUM_ARGS(), "O", &object, php_zmq_context_sc_entry);
	zend_restore_error_handling(&error_handling);

	if (parse_parameters_result != SUCCESS) {
		return;
	}

	intern = PHP_ZMQ_AUTH_OBJECT;

	context_object = php_zmq_context_fetch_object(Z_OBJ_P(object));

	// NOTE (phuedx, 2014-05-14): A zauth object needs a context so that it
	// can take over authentication for all incoming connections in that
	// context. Creating a shadow context from the specified context allows
	// us to continue working with CZMQ.
	intern->shadow_context = zctx_shadow_zmq_ctx(context_object->context->z_ctx);

	if (!intern->shadow_context) {
		zend_throw_exception_ex(php_zmq_auth_exception_sc_entry, PHP_ZMQ_INTERNAL_ERROR, "Failed to create the underlying shadow context object.");
		return;
	}

	intern->zauth = zauth_new(intern->shadow_context);

	if (!intern->zauth) {
		zctx_destroy(&intern->shadow_context);
		zend_throw_exception_ex(php_zmq_auth_exception_sc_entry, PHP_ZMQ_INTERNAL_ERROR, "Failed to create the underlying zauth object.");
	}
	return;
}

ZEND_BEGIN_ARG_INFO_EX(zmqauth___construct_args, 0, 0, 1)
	ZEND_ARG_OBJ_INFO(0, ZMQContext, ZMQContext, 0)
ZEND_END_ARG_INFO();

PHP_METHOD(zmqauth, allow)
{
	zend_string *address;
	php_zmq_auth_object *intern;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "S", &address) != SUCCESS) {
		return;
	}

	intern = PHP_ZMQ_AUTH_OBJECT;
	zauth_allow(intern->zauth, address->val);
	RETURN_ZVAL(getThis(), 1, 0);
}

ZEND_BEGIN_ARG_INFO_EX(zmqauth_allow_args, 0, 0, 1)
	ZEND_ARG_INFO(0, address)
ZEND_END_ARG_INFO();

PHP_METHOD(zmqauth, deny)
{
	zend_string *address;
	php_zmq_auth_object *intern;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "S", &address) != SUCCESS) {
		return;
	}

	intern = PHP_ZMQ_AUTH_OBJECT;
	zauth_deny(intern->zauth, address->val);
	RETURN_ZVAL(getThis(), 1, 0);
}

ZEND_BEGIN_ARG_INFO_EX(zmqauth_deny_args, 0, 0, 1)
	ZEND_ARG_INFO(0, address)
ZEND_END_ARG_INFO();

PHP_METHOD(zmqauth, configure)
{
	php_zmq_auth_object *intern;
	zend_long type;
	zend_string *domain;
	zend_string *filename;

	intern = PHP_ZMQ_AUTH_OBJECT

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "lSS", &type, &domain, &filename) != SUCCESS) {
		return;
	}

	intern = PHP_ZMQ_AUTH_OBJECT;

	switch (type) {
		case PHP_ZMQ_AUTH_TYPE_PLAIN:
			zauth_configure_plain(intern->zauth, domain->val, filename->val);
		break;

		case PHP_ZMQ_AUTH_TYPE_CURVE:
			zauth_configure_curve(intern->zauth, domain->val, filename->val);
		break;

		// TODO (phuedx, 2014-05-16): CZMQ now supports GSSAPI (see
		// zauth_configure_gssapi).

		default:
			zend_throw_exception_ex(php_zmq_auth_exception_sc_entry, PHP_ZMQ_INTERNAL_ERROR, "Unknown auth type. Are you using one of the ZMQAuth constants?");
		break;
	}

	RETURN_ZVAL(getThis(), 1, 0);
}

ZEND_BEGIN_ARG_INFO_EX(zmqauth_configure_args, 0, 0, 3)
	ZEND_ARG_INFO(0, type)
	ZEND_ARG_INFO(0, domain)
	ZEND_ARG_INFO(0, filename)
ZEND_END_ARG_INFO();

static zend_function_entry php_zmq_auth_class_methods[] = {
	PHP_ME(zmqauth, __construct,	zmqauth___construct_args,	ZEND_ACC_PUBLIC|ZEND_ACC_CTOR|ZEND_ACC_FINAL)
	PHP_ME(zmqauth, allow,			zmqauth_allow_args,			ZEND_ACC_PUBLIC)
	PHP_ME(zmqauth, deny,			zmqauth_deny_args,			ZEND_ACC_PUBLIC)
	PHP_ME(zmqauth, configure,		zmqauth_configure_args,		ZEND_ACC_PUBLIC)
	{NULL, NULL, NULL}
};

/* --- END ZMQAuth --- */

#endif // HAVE_CZMQ_2

ZEND_BEGIN_ARG_INFO_EX(zmq_construct_args, 0, 0, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_clock_args, 0, 0, 0)
ZEND_END_ARG_INFO()

#ifdef PHP_ZMQ_HAVE_Z85

ZEND_BEGIN_ARG_INFO_EX(zmq_z85encode_args, 0, 0, 1)
	ZEND_ARG_INFO(0, data)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_z85decode_args, 0, 0, 1)
	ZEND_ARG_INFO(0, data)
ZEND_END_ARG_INFO()

#endif

#ifdef PHP_ZMQ_HAVE_CURVE_KEYPAIR
ZEND_BEGIN_ARG_INFO_EX(zmq_curvekeypair_args, 0, 0, 0)
ZEND_END_ARG_INFO()
#endif

static zend_function_entry php_zmq_class_methods[] = {
	PHP_ME(zmq, __construct,    zmq_construct_args,  ZEND_ACC_PRIVATE|ZEND_ACC_CTOR)
	PHP_ME(zmq, clock,          zmq_clock_args,      ZEND_ACC_PUBLIC|ZEND_ACC_STATIC)
#ifdef PHP_ZMQ_HAVE_Z85
	PHP_ME(zmq, z85encode,      zmq_z85encode_args,  ZEND_ACC_PUBLIC|ZEND_ACC_STATIC)
	PHP_ME(zmq, z85decode,      zmq_z85decode_args,  ZEND_ACC_PUBLIC|ZEND_ACC_STATIC)
#endif
#ifdef PHP_ZMQ_HAVE_CURVE_KEYPAIR
	PHP_ME(zmq, curvekeypair,   zmq_curvekeypair_args,  ZEND_ACC_PUBLIC|ZEND_ACC_STATIC)
#endif
	{NULL, NULL, NULL}
};

ZEND_BEGIN_ARG_INFO_EX(zmq_context_construct_args, 0, 0, 0)
	ZEND_ARG_INFO(0, io_threads)
	ZEND_ARG_INFO(0, persistent)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_context_acquire_args, 0, 0, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_context_getsocketcount_args, 0, 0, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_context_getsocket_args, 0, 0, 2)
	ZEND_ARG_INFO(0, type)
	ZEND_ARG_INFO(0, dsn)
	ZEND_ARG_INFO(0, on_new_socket)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_context_ispersistent_args, 0, 0, 2)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_context_clone_args, 0, 0, 0)
ZEND_END_ARG_INFO()

#ifdef PHP_ZMQ_HAVE_CTX_OPTIONS
ZEND_BEGIN_ARG_INFO_EX(zmq_context_setopt_args, 0, 0, 2)
	ZEND_ARG_INFO(0, option)
	ZEND_ARG_INFO(0, value)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_context_getopt_args, 0, 0, 2)
	ZEND_ARG_INFO(0, option)
ZEND_END_ARG_INFO()
#endif

static zend_function_entry php_zmq_context_class_methods[] = {
	PHP_ME(zmqcontext, __construct,		zmq_context_construct_args,		ZEND_ACC_PUBLIC|ZEND_ACC_CTOR|ZEND_ACC_FINAL)
	PHP_ME(zmqcontext, acquire,			zmq_context_acquire_args,		ZEND_ACC_PUBLIC|ZEND_ACC_STATIC)
	PHP_ME(zmqcontext, getsocketcount,	zmq_context_getsocketcount_args,ZEND_ACC_PUBLIC)
	PHP_ME(zmqcontext, getsocket,		zmq_context_getsocket_args,		ZEND_ACC_PUBLIC)
	PHP_ME(zmqcontext, ispersistent,	zmq_context_ispersistent_args,	ZEND_ACC_PUBLIC)
	PHP_ME(zmqcontext, __clone,			zmq_context_clone_args,			ZEND_ACC_PRIVATE|ZEND_ACC_FINAL)
#ifdef PHP_ZMQ_HAVE_CTX_OPTIONS
	PHP_ME(zmqcontext, setOpt,			zmq_context_setopt_args,		ZEND_ACC_PUBLIC)
	PHP_ME(zmqcontext, getOpt,			zmq_context_getopt_args,		ZEND_ACC_PUBLIC)
#endif
	{NULL, NULL, NULL}
};

ZEND_BEGIN_ARG_INFO_EX(zmq_socket_construct_args, 0, 0, 2)
	ZEND_ARG_OBJ_INFO(0, ZMQContext, ZMQContext, 0)
	ZEND_ARG_INFO(0, type)
	ZEND_ARG_INFO(0, persistent_id)
	ZEND_ARG_INFO(0, on_new_socket)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_socket_bind_args, 0, 0, 1)
	ZEND_ARG_INFO(0, dsn)
	ZEND_ARG_INFO(0, force)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_socket_connect_args, 0, 0, 1)
	ZEND_ARG_INFO(0, dsn)
	ZEND_ARG_INFO(0, force)
ZEND_END_ARG_INFO()

#ifdef PHP_ZMQ_HAVE_SOCKET_MONITOR
ZEND_BEGIN_ARG_INFO_EX(zmq_socket_monitor_args, 0, 0, 1)
	ZEND_ARG_INFO(0, dsn)
	ZEND_ARG_INFO(0, events)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_socket_recvevent_args, 0, 0, 0)
	ZEND_ARG_INFO(0, flags)
ZEND_END_ARG_INFO()
#endif

#ifdef PHP_ZMQ_HAVE_UNBIND
ZEND_BEGIN_ARG_INFO_EX(zmq_socket_unbind_args, 0, 0, 1)
	ZEND_ARG_INFO(0, dsn)
ZEND_END_ARG_INFO()
#endif

#ifdef PHP_ZMQ_HAVE_DISCONNECT
ZEND_BEGIN_ARG_INFO_EX(zmq_socket_disconnect_args, 0, 0, 1)
	ZEND_ARG_INFO(0, dsn)
ZEND_END_ARG_INFO()
#endif

ZEND_BEGIN_ARG_INFO_EX(zmq_socket_setsockopt_args, 0, 0, 2)
	ZEND_ARG_INFO(0, key)
	ZEND_ARG_INFO(0, value)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_socket_getendpoints_args, 0, 0, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_socket_getsockettype_args, 0, 0, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_socket_send_args, 0, 0, 1)
	ZEND_ARG_INFO(0, message)
	ZEND_ARG_INFO(0, mode)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_socket_recv_args, 0, 0, 0)
	ZEND_ARG_INFO(0, mode)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_socket_getpersistentid_args, 0, 0, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_socket_getsockopt_args, 0, 0, 1)
	ZEND_ARG_INFO(0, key)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_socket_ispersistent_args, 0, 0, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_socket_clone_args, 0, 0, 0)
ZEND_END_ARG_INFO()

static zend_function_entry php_zmq_socket_class_methods[] = {
	PHP_ME(zmqsocket, __construct,			zmq_socket_construct_args,			ZEND_ACC_PUBLIC|ZEND_ACC_CTOR|ZEND_ACC_FINAL)
	PHP_ME(zmqsocket, send,					zmq_socket_send_args,				ZEND_ACC_PUBLIC)
	PHP_ME(zmqsocket, recv,					zmq_socket_recv_args,				ZEND_ACC_PUBLIC)
	PHP_ME(zmqsocket, sendmulti,			zmq_socket_send_args,				ZEND_ACC_PUBLIC)
	PHP_ME(zmqsocket, recvmulti,			zmq_socket_recv_args,				ZEND_ACC_PUBLIC)
	PHP_ME(zmqsocket, bind,					zmq_socket_bind_args,				ZEND_ACC_PUBLIC)
	PHP_ME(zmqsocket, connect,				zmq_socket_connect_args,			ZEND_ACC_PUBLIC)
#ifdef PHP_ZMQ_HAVE_SOCKET_MONITOR
	PHP_ME(zmqsocket, monitor,				zmq_socket_monitor_args,			ZEND_ACC_PUBLIC)
	PHP_ME(zmqsocket, recvevent,			zmq_socket_recvevent_args,			ZEND_ACC_PUBLIC)
#endif
#ifdef PHP_ZMQ_HAVE_UNBIND
	PHP_ME(zmqsocket, unbind,				zmq_socket_unbind_args,				ZEND_ACC_PUBLIC)
#endif
#ifdef PHP_ZMQ_HAVE_DISCONNECT
	PHP_ME(zmqsocket, disconnect,			zmq_socket_disconnect_args,			ZEND_ACC_PUBLIC)
#endif
	PHP_ME(zmqsocket, setsockopt,			zmq_socket_setsockopt_args,			ZEND_ACC_PUBLIC)
	PHP_ME(zmqsocket, getendpoints,			zmq_socket_getendpoints_args,		ZEND_ACC_PUBLIC)
	PHP_ME(zmqsocket, getsockettype,		zmq_socket_getsockettype_args,		ZEND_ACC_PUBLIC)
	PHP_ME(zmqsocket, ispersistent,			zmq_socket_ispersistent_args,		ZEND_ACC_PUBLIC)
	PHP_ME(zmqsocket, getpersistentid,		zmq_socket_getpersistentid_args,	ZEND_ACC_PUBLIC)
	PHP_ME(zmqsocket, getsockopt,			zmq_socket_getsockopt_args,			ZEND_ACC_PUBLIC)
	PHP_ME(zmqsocket, __clone,				zmq_socket_clone_args,				ZEND_ACC_PRIVATE|ZEND_ACC_FINAL)
	PHP_MALIAS(zmqsocket,	sendmsg, send,	zmq_socket_send_args, 				ZEND_ACC_PUBLIC)
	PHP_MALIAS(zmqsocket,	recvmsg, recv, 	zmq_socket_recv_args, 				ZEND_ACC_PUBLIC)
	{NULL, NULL, NULL}
};

ZEND_BEGIN_ARG_INFO_EX(zmq_poll_add_args, 0, 0, 2)
	ZEND_ARG_INFO(0, entry)
	ZEND_ARG_INFO(0, type)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_poll_poll_args, 0, 0, 2)
	ZEND_ARG_INFO(1, readable)
	ZEND_ARG_INFO(1, writable)
	ZEND_ARG_INFO(0, timeout)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_poll_getlasterrors_args, 0, 0, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_poll_remove_args, 0, 0, 2)
	ZEND_ARG_INFO(0, remove)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_poll_count_args, 0, 0, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_poll_clear_args, 0, 0, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_poll_items_args, 0, 0, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_poll_clone_args, 0, 0, 0)
ZEND_END_ARG_INFO()

static zend_function_entry php_zmq_poll_class_methods[] = {
	PHP_ME(zmqpoll, add,			zmq_poll_add_args,				ZEND_ACC_PUBLIC)
	PHP_ME(zmqpoll, poll,			zmq_poll_poll_args,				ZEND_ACC_PUBLIC)
	PHP_ME(zmqpoll, getlasterrors,	zmq_poll_getlasterrors_args,	ZEND_ACC_PUBLIC)
	PHP_ME(zmqpoll, remove,			zmq_poll_remove_args,			ZEND_ACC_PUBLIC)
	PHP_ME(zmqpoll, count,			zmq_poll_count_args,			ZEND_ACC_PUBLIC)
	PHP_ME(zmqpoll, clear,			zmq_poll_clear_args,			ZEND_ACC_PUBLIC)
	PHP_ME(zmqpoll, items,			zmq_poll_items_args,			ZEND_ACC_PUBLIC)
	PHP_ME(zmqpoll, __clone,		zmq_poll_clone_args,			ZEND_ACC_PRIVATE|ZEND_ACC_FINAL)
	{NULL, NULL, NULL}
};

ZEND_BEGIN_ARG_INFO_EX(zmq_device_construct_args, 0, 0, 2)
	ZEND_ARG_OBJ_INFO(0, frontend, ZMQSocket, 0)
	ZEND_ARG_OBJ_INFO(0, backend, ZMQSocket, 0)
	ZEND_ARG_OBJ_INFO(0, capture, ZMQSocket, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_device_run_args, 0, 0, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_device_setidlecallback_args, 0, 0, 2)
	ZEND_ARG_INFO(0, idle_callback)
	ZEND_ARG_INFO(0, timeout)
	ZEND_ARG_INFO(0, user_data)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_device_settimercallback_args, 0, 0, 2)
	ZEND_ARG_INFO(0, idle_callback)
	ZEND_ARG_INFO(0, timeout)
	ZEND_ARG_INFO(0, user_data)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_device_setidletimeout_args, 0, 0, 1)
	ZEND_ARG_INFO(0, timeout)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_device_settimertimeout_args, 0, 0, 1)
	ZEND_ARG_INFO(0, timeout)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_device_getidletimeout_args, 0, 0, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_device_gettimertimeout_args, 0, 0, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(zmq_device_clone_args, 0, 0, 0)
ZEND_END_ARG_INFO()

static zend_function_entry php_zmq_device_class_methods[] = {
	PHP_ME(zmqdevice, __construct,      zmq_device_construct_args,         ZEND_ACC_PUBLIC|ZEND_ACC_CTOR|ZEND_ACC_FINAL)
	PHP_ME(zmqdevice, run,              zmq_device_run_args,               ZEND_ACC_PUBLIC)
	PHP_ME(zmqdevice, setidlecallback,  zmq_device_setidlecallback_args,   ZEND_ACC_PUBLIC)
	PHP_ME(zmqdevice, setidletimeout,   zmq_device_setidletimeout_args,    ZEND_ACC_PUBLIC)
	PHP_ME(zmqdevice, getidletimeout,   zmq_device_getidletimeout_args,    ZEND_ACC_PUBLIC)
	PHP_ME(zmqdevice, settimercallback, zmq_device_settimercallback_args,  ZEND_ACC_PUBLIC)
	PHP_ME(zmqdevice, settimertimeout,  zmq_device_settimertimeout_args,   ZEND_ACC_PUBLIC)
	PHP_ME(zmqdevice, gettimertimeout,  zmq_device_gettimertimeout_args,   ZEND_ACC_PUBLIC)
	PHP_ME(zmqdevice, __clone,          zmq_device_clone_args,             ZEND_ACC_PRIVATE|ZEND_ACC_FINAL)
	{NULL, NULL, NULL}
};


zend_function_entry zmq_functions[] = {
	{NULL, NULL, NULL}
};

static
void php_zmq_context_object_free_storage(zend_object *object)
{
	php_zmq_context_object *intern = php_zmq_context_fetch_object(object);

	if (!intern) {
		return;
	}

	if (intern->context) {
		if (!intern->context->is_persistent) {
			php_zmq_context_destroy(intern->context);
		}
	}
	zend_object_std_dtor(&intern->zo);
}

static
void php_zmq_socket_object_free_storage(zend_object *object)
{
	php_zmq_socket_object *intern = php_zmq_socket_fetch_object(object);

	if (!intern) {
		return;
	}

	if (intern->socket) {
		if (intern->socket->is_persistent && intern->persistent_id) {
			efree(intern->persistent_id);
		}
		if (!intern->socket->is_persistent) {
			php_zmq_socket_destroy(intern->socket);
		}
	}

	if (!Z_ISUNDEF(intern->context_obj)) {
		zval_ptr_dtor(&intern->context_obj);
	}
	zend_object_std_dtor(&intern->zo);
}

static
void php_zmq_poll_object_free_storage(zend_object *object)
{
	php_zmq_poll_object *intern = php_zmq_poll_fetch_object(object);

	if (!intern) {
		return;
	}
	php_zmq_pollset_destroy(&intern->set);
	zend_object_std_dtor(&intern->zo);
}

static void php_zmq_device_object_free_storage(zend_object *object)
{
	php_zmq_device_object *intern = php_zmq_device_fetch_object(object);

	if (!intern) {
		return;
	}

	s_clear_device_callback (&intern->idle_cb);
	s_clear_device_callback (&intern->timer_cb);

	if (!Z_ISUNDEF(intern->front)) {
		zval_ptr_dtor(&intern->front);
	}

	if (!Z_ISUNDEF(intern->back)) {
		zval_ptr_dtor(&intern->back);
	}

	if (!Z_ISUNDEF(intern->capture)) {
		zval_ptr_dtor(&intern->capture);
	}
	zend_object_std_dtor(&intern->zo);
}

static
zend_object *php_zmq_context_object_new_ex(zend_class_entry *class_type, php_zmq_context_object **ptr)
{
	php_zmq_context_object *intern = ecalloc(1, sizeof(php_zmq_context_object) + zend_object_properties_size(class_type));

	/* Context is initialized in the constructor */
	intern->context = NULL;

	if (ptr) {
		*ptr = intern;
	}

	zend_object_std_init(&intern->zo, class_type);
	object_properties_init(&intern->zo, class_type);

	intern->zo.handlers = &zmq_context_object_handlers;
	return &intern->zo;
}

static
zend_object *php_zmq_socket_object_new_ex(zend_class_entry *class_type, php_zmq_socket_object **ptr)
{
	php_zmq_socket_object *intern;

	/* Allocate memory for it */
	intern = ecalloc(1, sizeof(php_zmq_socket_object) + zend_object_properties_size(class_type));

	intern->socket        = NULL;
	intern->persistent_id = NULL;
	ZVAL_UNDEF(&intern->context_obj);

	if (ptr) {
		*ptr = intern;
	}

	zend_object_std_init(&intern->zo, class_type);
	object_properties_init(&intern->zo, class_type);

	intern->zo.handlers = &zmq_socket_object_handlers;
	return &intern->zo;
}

static
zend_object *php_zmq_poll_object_new_ex(zend_class_entry *class_type, php_zmq_poll_object **ptr)
{
	php_zmq_poll_object *intern = ecalloc(1, sizeof(php_zmq_poll_object) + zend_object_properties_size(class_type));
	intern->set = php_zmq_pollset_init();

	if (ptr) {
		*ptr = intern;
	}

	zend_object_std_init(&intern->zo, class_type);
	object_properties_init(&intern->zo, class_type);

	intern->zo.handlers = &zmq_poll_object_handlers;
	return &intern->zo;
}

static
zend_object *php_zmq_device_object_new_ex(zend_class_entry *class_type, php_zmq_device_object **ptr)
{
	php_zmq_device_object *intern = ecalloc(1, sizeof(php_zmq_device_object) + zend_object_properties_size(class_type));

	memset (&intern->idle_cb, 0, sizeof (php_zmq_device_cb_t));
	memset (&intern->timer_cb, 0, sizeof (php_zmq_device_cb_t));

	ZVAL_UNDEF(&intern->front);
	ZVAL_UNDEF(&intern->back);
	ZVAL_UNDEF(&intern->capture);

	if (ptr) {
		*ptr = intern;
	}

	zend_object_std_init(&intern->zo, class_type);
	object_properties_init(&intern->zo, class_type);

	intern->zo.handlers = &zmq_device_object_handlers;
	return &intern->zo;
}

static
zend_object *php_zmq_context_object_new(zend_class_entry *class_type)
{
	return php_zmq_context_object_new_ex(class_type, NULL);
}

static
zend_object *php_zmq_socket_object_new(zend_class_entry *class_type)
{
	return php_zmq_socket_object_new_ex(class_type, NULL);
}

static
zend_object *php_zmq_poll_object_new(zend_class_entry *class_type)
{
	return php_zmq_poll_object_new_ex(class_type, NULL);
}

static
zend_object *php_zmq_device_object_new(zend_class_entry *class_type)
{
	return php_zmq_device_object_new_ex(class_type, NULL);
}

ZEND_RSRC_DTOR_FUNC(php_zmq_context_dtor)
{
	if (res->ptr) {
		php_zmq_context *ctx = (php_zmq_context *)res->ptr;
		php_zmq_context_destroy(ctx);
		res->ptr = NULL;
	}
}

ZEND_RSRC_DTOR_FUNC(php_zmq_socket_dtor)
{
	if (res->ptr) {
		php_zmq_socket *zms = (php_zmq_socket *)res->ptr;
		php_zmq_socket_destroy(zms);
		res->ptr = NULL;
	}
}

static
void php_zmq_init_globals (zend_php_zmq_globals *zmq_globals)
{
	zmq_globals->clock_ctx = NULL;
}

PHP_MINIT_FUNCTION(zmq)
{
	char *version;
	zend_class_entry ce, ce_context, ce_socket, ce_poll, ce_device;
	zend_class_entry ce_exception, ce_context_exception, ce_socket_exception, ce_poll_exception, ce_device_exception;

#ifdef HAVE_CZMQ_2
	zend_class_entry ce_cert, ce_cert_exception, ce_auth, ce_auth_exception;
#endif

	le_zmq_context = zend_register_list_destructors_ex(NULL, php_zmq_context_dtor, "ZMQ persistent context", module_number);
	le_zmq_socket  = zend_register_list_destructors_ex(NULL, php_zmq_socket_dtor, "ZMQ persistent socket", module_number);

	memcpy(&zmq_object_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
	memcpy(&zmq_context_object_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
	memcpy(&zmq_socket_object_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
	memcpy(&zmq_poll_object_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
	memcpy(&zmq_device_object_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));

#ifdef HAVE_CZMQ_2
	memcpy(&zmq_cert_object_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
	memcpy(&zmq_auth_object_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
#endif

	INIT_CLASS_ENTRY(ce, "ZMQ", php_zmq_class_methods);
	ce.create_object = NULL;
	zmq_object_handlers.clone_obj = NULL;
	php_zmq_sc_entry = zend_register_internal_class(&ce);

	INIT_CLASS_ENTRY(ce_context, "ZMQContext", php_zmq_context_class_methods);
	ce_context.create_object = php_zmq_context_object_new;

	zmq_context_object_handlers.offset    = XtOffsetOf(php_zmq_context_object, zo);
	zmq_context_object_handlers.clone_obj = NULL;
	zmq_context_object_handlers.free_obj  = php_zmq_context_object_free_storage;

	php_zmq_context_sc_entry = zend_register_internal_class(&ce_context);

	INIT_CLASS_ENTRY(ce_socket, "ZMQSocket", php_zmq_socket_class_methods);
	ce_socket.create_object = php_zmq_socket_object_new;

	zmq_socket_object_handlers.offset    = XtOffsetOf(php_zmq_socket_object, zo);
	zmq_socket_object_handlers.clone_obj = NULL;
	zmq_socket_object_handlers.free_obj  = php_zmq_socket_object_free_storage;

	php_zmq_socket_sc_entry = zend_register_internal_class(&ce_socket);

	INIT_CLASS_ENTRY(ce_poll, "ZMQPoll", php_zmq_poll_class_methods);
	ce_poll.create_object = php_zmq_poll_object_new;

	zmq_poll_object_handlers.offset    = XtOffsetOf(php_zmq_poll_object, zo);
	zmq_poll_object_handlers.clone_obj = NULL;
	zmq_poll_object_handlers.free_obj  = php_zmq_poll_object_free_storage;

	php_zmq_poll_sc_entry = zend_register_internal_class(&ce_poll);

	INIT_CLASS_ENTRY(ce_device, "ZMQDevice", php_zmq_device_class_methods);
	ce_device.create_object = php_zmq_device_object_new;

	zmq_device_object_handlers.offset    = XtOffsetOf(php_zmq_device_object, zo);
	zmq_device_object_handlers.clone_obj = NULL;
	zmq_device_object_handlers.free_obj  = php_zmq_device_object_free_storage;

	php_zmq_device_sc_entry = zend_register_internal_class(&ce_device);

#ifdef HAVE_CZMQ_2
	INIT_CLASS_ENTRY(ce_cert, "ZMQCert", php_zmq_cert_class_methods);
	ce_cert.create_object = php_zmq_cert_new;

	zmq_cert_object_handlers.offset    = XtOffsetOf(php_zmq_cert_object, zo);
	zmq_cert_object_handlers.clone_obj = php_zmq_cert_clone;
	zmq_cert_object_handlers.free_obj  = php_zmq_cert_object_free_storage;

	php_zmq_cert_sc_entry = zend_register_internal_class(&ce_cert);

	INIT_CLASS_ENTRY(ce_auth, "ZMQAuth", php_zmq_auth_class_methods);
	ce_auth.create_object = php_zmq_auth_new;

	zmq_auth_object_handlers.offset    = XtOffsetOf(php_zmq_auth_object, zo);
	zmq_auth_object_handlers.clone_obj = NULL;
	zmq_auth_object_handlers.free_obj  = php_zmq_auth_object_free_storage;

	php_zmq_auth_sc_entry = zend_register_internal_class(&ce_auth);
#endif

	INIT_CLASS_ENTRY(ce_exception, "ZMQException", NULL);
	php_zmq_exception_sc_entry = zend_register_internal_class_ex(&ce_exception, zend_exception_get_default());
	php_zmq_exception_sc_entry->ce_flags &= ~ZEND_ACC_FINAL;

	INIT_CLASS_ENTRY(ce_context_exception, "ZMQContextException", NULL);
	php_zmq_context_exception_sc_entry = zend_register_internal_class_ex(&ce_context_exception, php_zmq_exception_sc_entry);
	php_zmq_context_exception_sc_entry->ce_flags |= ZEND_ACC_FINAL;

	INIT_CLASS_ENTRY(ce_socket_exception, "ZMQSocketException", NULL);
	php_zmq_socket_exception_sc_entry = zend_register_internal_class_ex(&ce_socket_exception, php_zmq_exception_sc_entry);
	php_zmq_socket_exception_sc_entry->ce_flags |= ZEND_ACC_FINAL;

	INIT_CLASS_ENTRY(ce_poll_exception, "ZMQPollException", NULL);
	php_zmq_poll_exception_sc_entry = zend_register_internal_class_ex(&ce_poll_exception, php_zmq_exception_sc_entry);
	php_zmq_poll_exception_sc_entry->ce_flags |= ZEND_ACC_FINAL;

	INIT_CLASS_ENTRY(ce_device_exception, "ZMQDeviceException", NULL);
	php_zmq_device_exception_sc_entry = zend_register_internal_class_ex(&ce_device_exception, php_zmq_exception_sc_entry);
	php_zmq_device_exception_sc_entry->ce_flags |= ZEND_ACC_FINAL;

#ifdef HAVE_CZMQ_2
	INIT_CLASS_ENTRY(ce_cert_exception, "ZMQCertException", NULL);
	php_zmq_cert_exception_sc_entry = zend_register_internal_class_ex(&ce_cert_exception, php_zmq_exception_sc_entry);
	php_zmq_cert_exception_sc_entry->ce_flags |= ZEND_ACC_FINAL;

	INIT_CLASS_ENTRY(ce_auth_exception, "ZMQAuthException", NULL);
	php_zmq_auth_exception_sc_entry = zend_register_internal_class_ex(&ce_auth_exception, php_zmq_exception_sc_entry);
	php_zmq_auth_exception_sc_entry->ce_flags |= ZEND_ACC_FINAL;
#endif

	ZEND_INIT_MODULE_GLOBALS(php_zmq, php_zmq_init_globals, NULL);

	ZMQ_G(clock_ctx) = php_zmq_clock_init ();

	if (!ZMQ_G(clock_ctx)) {
		php_error_docref(NULL, E_WARNING, "Failed to initialise clock");
		return FAILURE;
	}

#define PHP_ZMQ_REGISTER_CONST_LONG(const_name, value) \
	zend_declare_class_constant_long(php_zmq_sc_entry, const_name, sizeof(const_name)-1, (long)value);
#define PHP_ZMQ_REGISTER_CONST_STRING(const_name, value) \
	zend_declare_class_constant_string (php_zmq_sc_entry, const_name, sizeof(const_name)-1, value);
#define PHP_ZMQ_REGISTER_CONST_BOOL(const_name, value) \
	zend_declare_class_constant_bool (php_zmq_sc_entry, const_name, sizeof(const_name)-1, value);

	/* Socket constants */
	PHP_ZMQ_REGISTER_CONST_LONG("SOCKET_PAIR", ZMQ_PAIR);
	PHP_ZMQ_REGISTER_CONST_LONG("SOCKET_PUB", ZMQ_PUB);
	PHP_ZMQ_REGISTER_CONST_LONG("SOCKET_SUB", ZMQ_SUB);
	PHP_ZMQ_REGISTER_CONST_LONG("SOCKET_REQ", ZMQ_REQ);
	PHP_ZMQ_REGISTER_CONST_LONG("SOCKET_REP", ZMQ_REP);
	PHP_ZMQ_REGISTER_CONST_LONG("SOCKET_XREQ", ZMQ_XREQ);
	PHP_ZMQ_REGISTER_CONST_LONG("SOCKET_XREP", ZMQ_XREP);
	PHP_ZMQ_REGISTER_CONST_LONG("SOCKET_PUSH", ZMQ_PUSH);
	PHP_ZMQ_REGISTER_CONST_LONG("SOCKET_PULL", ZMQ_PULL);
	PHP_ZMQ_REGISTER_CONST_LONG("SOCKET_DEALER", ZMQ_DEALER);
	PHP_ZMQ_REGISTER_CONST_LONG("SOCKET_ROUTER", ZMQ_ROUTER);
#ifdef ZMQ_XSUB
	PHP_ZMQ_REGISTER_CONST_LONG("SOCKET_XSUB", ZMQ_XSUB);
#endif
#ifdef ZMQ_XPUB
	PHP_ZMQ_REGISTER_CONST_LONG("SOCKET_XPUB", ZMQ_XPUB);
#endif
#ifdef ZMQ_STREAM
	PHP_ZMQ_REGISTER_CONST_LONG("SOCKET_STREAM", ZMQ_STREAM);
#endif

	/* 2.0? */
	PHP_ZMQ_REGISTER_CONST_LONG("SOCKET_UPSTREAM", ZMQ_PULL);
	PHP_ZMQ_REGISTER_CONST_LONG("SOCKET_DOWNSTREAM", ZMQ_PUSH);

#ifdef ZMQ_SNDLABEL
	PHP_ZMQ_REGISTER_CONST_LONG("MODE_SNDLABEL", ZMQ_SNDLABEL);
#endif

	PHP_ZMQ_REGISTER_CONST_LONG("POLL_IN", ZMQ_POLLIN);
	PHP_ZMQ_REGISTER_CONST_LONG("POLL_OUT", ZMQ_POLLOUT);

	PHP_ZMQ_REGISTER_CONST_LONG("MODE_SNDMORE", ZMQ_SNDMORE);
	PHP_ZMQ_REGISTER_CONST_LONG("MODE_NOBLOCK", ZMQ_DONTWAIT);
	PHP_ZMQ_REGISTER_CONST_LONG("MODE_DONTWAIT", ZMQ_DONTWAIT);

	PHP_ZMQ_REGISTER_CONST_LONG("ERR_INTERNAL", PHP_ZMQ_INTERNAL_ERROR);
	PHP_ZMQ_REGISTER_CONST_LONG("ERR_EAGAIN", EAGAIN);
	PHP_ZMQ_REGISTER_CONST_LONG("ERR_ENOTSUP", ENOTSUP);
	PHP_ZMQ_REGISTER_CONST_LONG("ERR_EFSM", EFSM);
	PHP_ZMQ_REGISTER_CONST_LONG("ERR_ETERM", ETERM);

	version = php_zmq_get_libzmq_version();

	PHP_ZMQ_REGISTER_CONST_STRING("LIBZMQ_VER",     version);
	PHP_ZMQ_REGISTER_CONST_STRING("LIBZMQ_VERSION", version);
	PHP_ZMQ_REGISTER_CONST_LONG("LIBZMQ_VERSION_ID", php_zmq_get_libzmq_version_id());

	PHP_ZMQ_REGISTER_CONST_LONG("LIBZMQ_VERSION_MAJOR", ZMQ_VERSION_MAJOR);
	PHP_ZMQ_REGISTER_CONST_LONG("LIBZMQ_VERSION_MINOR", ZMQ_VERSION_MINOR);
	PHP_ZMQ_REGISTER_CONST_LONG("LIBZMQ_VERSION_PATCH", ZMQ_VERSION_PATCH);

	efree(version);

	php_zmq_register_sockopt_constants (php_zmq_sc_entry);

#ifdef ZMQ_MAX_SOCKETS
	PHP_ZMQ_REGISTER_CONST_LONG("CTXOPT_MAX_SOCKETS", ZMQ_MAX_SOCKETS);
#endif

#ifdef ZMQ_MAX_SOCKETS_DFLT
	PHP_ZMQ_REGISTER_CONST_LONG("CTXOPT_MAX_SOCKETS_DEFAULT", ZMQ_MAX_SOCKETS_DFLT);
#endif

#ifdef HAVE_CZMQ_2
	PHP_ZMQ_REGISTER_CONST_STRING("CURVE_ALLOW_ANY", CURVE_ALLOW_ANY);

	zend_declare_class_constant_long(php_zmq_auth_sc_entry, "AUTH_TYPE_PLAIN", 15, (long)PHP_ZMQ_AUTH_TYPE_PLAIN);
	zend_declare_class_constant_long(php_zmq_auth_sc_entry, "AUTH_TYPE_CURVE", 15, (long)PHP_ZMQ_AUTH_TYPE_CURVE);
#endif

#ifdef PHP_ZMQ_HAVE_SOCKET_MONITOR
	PHP_ZMQ_REGISTER_CONST_LONG("EVENT_CONNECTED",       ZMQ_EVENT_CONNECTED);
	PHP_ZMQ_REGISTER_CONST_LONG("EVENT_CONNECT_DELAYED", ZMQ_EVENT_CONNECT_DELAYED);
	PHP_ZMQ_REGISTER_CONST_LONG("EVENT_CONNECT_RETRIED", ZMQ_EVENT_CONNECT_RETRIED);
	PHP_ZMQ_REGISTER_CONST_LONG("EVENT_LISTENING",       ZMQ_EVENT_LISTENING);
	PHP_ZMQ_REGISTER_CONST_LONG("EVENT_BIND_FAILED",     ZMQ_EVENT_BIND_FAILED);
	PHP_ZMQ_REGISTER_CONST_LONG("EVENT_ACCEPTED",        ZMQ_EVENT_ACCEPTED);
	PHP_ZMQ_REGISTER_CONST_LONG("EVENT_ACCEPT_FAILED",   ZMQ_EVENT_ACCEPT_FAILED);
	PHP_ZMQ_REGISTER_CONST_LONG("EVENT_CLOSED",          ZMQ_EVENT_CLOSED);
	PHP_ZMQ_REGISTER_CONST_LONG("EVENT_CLOSE_FAILED",    ZMQ_EVENT_CLOSE_FAILED);
	PHP_ZMQ_REGISTER_CONST_LONG("EVENT_DISCONNECTED",    ZMQ_EVENT_DISCONNECTED);
	PHP_ZMQ_REGISTER_CONST_LONG("EVENT_MONITOR_STOPPED", ZMQ_EVENT_MONITOR_STOPPED);
	PHP_ZMQ_REGISTER_CONST_LONG("EVENT_ALL",             ZMQ_EVENT_ALL);
#endif

#undef PHP_ZMQ_REGISTER_CONST_LONG
#undef PHP_ZMQ_REGISTER_CONST_STRING

	if (!php_zmq_shared_ctx_init()) {
		return FAILURE;
	}
	return SUCCESS;
}

PHP_MSHUTDOWN_FUNCTION(zmq)
{
	php_zmq_shared_ctx_destroy();
	php_zmq_clock_destroy (&ZMQ_G (clock_ctx));
	return SUCCESS;
}

PHP_MINFO_FUNCTION(zmq)
{
	char *version = php_zmq_get_libzmq_version();

	php_info_print_table_start();

		php_info_print_table_header(2, "ZMQ extension", "enabled");
		php_info_print_table_row(2, "ZMQ extension version", PHP_ZMQ_VERSION);
		php_info_print_table_row(2, "libzmq version", version);

	php_info_print_table_end();
	DISPLAY_INI_ENTRIES();

	efree(version);
}

zend_module_entry zmq_module_entry =
{
	STANDARD_MODULE_HEADER,
	PHP_ZMQ_EXTNAME,
	zmq_functions,			/* Functions */
	PHP_MINIT(zmq),			/* MINIT */
	PHP_MSHUTDOWN(zmq),		/* MSHUTDOWN */
	NULL,					/* RINIT */
	NULL,					/* RSHUTDOWN */
	PHP_MINFO(zmq),			/* MINFO */
	PHP_ZMQ_VERSION,		/* version */
	STANDARD_MODULE_PROPERTIES
};

#ifdef COMPILE_DL_ZMQ
ZEND_GET_MODULE(zmq)
#endif /* COMPILE_DL_ZMQ */
