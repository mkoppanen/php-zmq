PHP_ARG_WITH(zmq,     whether to enable 0MQ support,
[  --with-zmq[=DIR]   Enable 0MQ support. DIR is the prefix to libzmq installation directory.], yes)

PHP_ARG_ENABLE(zmq_pthreads,    whether to enable support for php threads extension,
[  --enable-zmq-pthreads        whether to enable support for php threads extension], no, no)

PHP_ARG_WITH(czmq,    whether to enable CZMQ support,
[  --with-czmq[=DIR]  Enable CZMQ support. DIR is the prefix to CZMQ installation directory.], no, no)

PHP_ARG_WITH(zyre,    whether to enable Zyre support,
[  --with-zyre[=DIR]  Enable Zyre support. DIR is the prefix to Zyre installation directory.], no, no)

if test "$PHP_ZMQ" != "no"; then

  AC_PATH_PROG(PKG_CONFIG, pkg-config, no)
  if test "x$PKG_CONFIG" = "xno"; then
    AC_MSG_RESULT([pkg-config not found])
    AC_MSG_ERROR([Please reinstall the pkg-config distribution])
  fi

  ORIG_PKG_CONFIG_PATH=$PKG_CONFIG_PATH

  AC_MSG_CHECKING(libzmq installation)
  if test "x$PHP_ZMQ" = "xyes"; then
    if test "x${PKG_CONFIG_PATH}" = "x"; then
      #
      # "By default, pkg-config looks in the directory prefix/lib/pkgconfig for these files"
      #
      # Add a bit more search paths for common installation locations. Can be overridden by setting
      # PKG_CONFIG_PATH env variable or passing --with-zmq=PATH
      #
      export PKG_CONFIG_PATH="/usr/local/${PHP_LIBDIR}/pkgconfig:/usr/${PHP_LIBDIR}/pkgconfig:/opt/${PHP_LIBDIR}/pkgconfig:/opt/local/${PHP_LIBDIR}/pkgconfig"
    fi
  else
    export PKG_CONFIG_PATH="${PHP_ZMQ}/${PHP_LIBDIR}/pkgconfig"
  fi

  if $PKG_CONFIG --exists libzmq; then
    PHP_ZMQ_VERSION=`$PKG_CONFIG libzmq --modversion`
    PHP_ZMQ_PREFIX=`$PKG_CONFIG libzmq --variable=prefix`

    AC_MSG_RESULT([found version $PHP_ZMQ_VERSION, under $PHP_ZMQ_PREFIX])
    PHP_ZMQ_LIBS=`$PKG_CONFIG libzmq --libs`
    PHP_ZMQ_INCS=`$PKG_CONFIG libzmq --cflags`

    PHP_EVAL_LIBLINE($PHP_ZMQ_LIBS, ZMQ_SHARED_LIBADD)
    PHP_EVAL_INCLINE($PHP_ZMQ_INCS)
  else
    AC_MSG_ERROR(Unable to find libzmq installation)
  fi

  if test "$PHP_CZMQ" != "no"; then
    if test "x$PHP_CZMQ" != "xyes"; then
      export PKG_CONFIG_PATH="${PHP_CZMQ}/${PHP_LIBDIR}/pkgconfig"
    fi

    AC_MSG_CHECKING(for CZMQ)
    if $PKG_CONFIG --exists libczmq; then
      PHP_CZMQ_VERSION=`$PKG_CONFIG libczmq --modversion`
      PHP_CZMQ_PREFIX=`$PKG_CONFIG libczmq --variable=prefix`
      AC_MSG_RESULT([found version $PHP_CZMQ_VERSION in $PHP_CZMQ_PREFIX])

      PHP_CZMQ_LIBS=`$PKG_CONFIG libczmq --libs`
      PHP_CZMQ_INCS=`$PKG_CONFIG libczmq --cflags`

      PHP_EVAL_LIBLINE($PHP_CZMQ_LIBS, ZMQ_SHARED_LIBADD)
      PHP_EVAL_INCLINE($PHP_CZMQ_INCS)

      AC_DEFINE([HAVE_CZMQ], [], [CZMQ was found])
    else
      AC_MSG_RESULT([no])
    fi
  fi

  if test "$PHP_ZYRE" != "no" -a "$PHP_CZMQ" != "no"; then
    if test "x$PHP_ZYRE" != "xyes"; then
      export PKG_CONFIG_PATH="${PHP_ZYRE}/${PHP_LIBDIR}/pkgconfig"
    fi

    AC_MSG_CHECKING(for Zyre)
    if $PKG_CONFIG --exists libzyre; then
      PHP_ZYRE_VERSION=`$PKG_CONFIG libzyre --modversion`
      PHP_ZYRE_PREFIX=`$PKG_CONFIG libzyre --variable=prefix`
      AC_MSG_RESULT([found version $PHP_ZYRE_VERSION in $PHP_ZYRE_PREFIX])

      PHP_ZYRE_LIBS=`$PKG_CONFIG libzyre --libs`
      PHP_ZYRE_INCS=`$PKG_CONFIG libzyre --cflags`

      PHP_EVAL_LIBLINE($PHP_ZYRE_LIBS, ZMQ_SHARED_LIBADD)
      PHP_EVAL_INCLINE($PHP_ZYRE_INCS)

      AC_DEFINE([HAVE_ZYRE], [], [ZYRE was found])
    else
      AC_MSG_RESULT([no])
    fi
  fi

  AC_CHECK_HEADERS([stdint.h],[php_zmq_have_stdint=yes; break;])
  if test $php_zmq_have_stdint != "yes"; then
    AC_MSG_ERROR(Unable to find stdint.h)
  fi

  if test "x$PHP_ZMQ_PTHREADS" = "xyes"; then
    AC_DEFINE(PHP_ZMQ_PTHREADS, 1, [Enable support for phtreads])
  fi

  AC_CHECK_HEADERS(time.h sys/time.h mach/mach_time.h)
  AC_SEARCH_LIBS(clock_gettime, rt)
  AC_CHECK_FUNCS(clock_gettime gettimeofday mach_absolute_time)

  PHP_SUBST(ZMQ_SHARED_LIBADD)
  PHP_NEW_EXTENSION(zmq, zmq.c zmq_pollset.c zmq_device.c zmq_sockopt.c zmq_fd_stream.c zmq_clock.c, $ext_shared)
  PKG_CONFIG_PATH="$ORIG_PKG_CONFIG_PATH"
fi
