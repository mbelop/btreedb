PROG=	btree

SRCS=	btree.c main.c

CFLAGS+=-Wall -O0 -DDEBUG -g

LDADD+=	-lcrypto
DPADD+=	${LIBCRYPTO}

NOMAN=

.include <bsd.prog.mk>
