# ===== Compiler & Flags =====
CC       = gcc
RM       = rm -f

# enable sanitizers with:  make clean && make SAN=asan
SANFLAGS =
ifeq ($(SAN),asan)
SANFLAGS = -fsanitize=address,undefined -fno-omit-frame-pointer
endif

CFLAGS   = -g -Wall $(SANFLAGS)
LDFLAGS  = $(SANFLAGS) -lm

# Optional page file argument to your program:
#   make run ARG=mydata.bin
ARG ?=

.PHONY: all default clean run run_arg run_expr asan asan-run lldb help

# ===== Build graph =====
default: recordmgr
all: recordmgr

recordmgr: test_assign3_1.o dberror.o expr.o record_mgr.o rm_serializer.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

test_expr: test_expr.o dberror.o expr.o record_mgr.o rm_serializer.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

# ===== Object rules =====
test_assign3_1.o: test_assign3_1.c dberror.h storage_mgr.h test_helper.h buffer_mgr.h buffer_mgr_stat.h
	$(CC) $(CFLAGS) -c $<

test_expr.o: test_expr.c dberror.h expr.h record_mgr.h tables.h test_helper.h
	$(CC) $(CFLAGS) -c $<

record_mgr.o: record_mgr.c record_mgr.h buffer_mgr.h storage_mgr.h
	$(CC) $(CFLAGS) -c $<

expr.o: expr.c dberror.h record_mgr.h expr.h tables.h
	$(CC) $(CFLAGS) -c $<

rm_serializer.o: rm_serializer.c dberror.h tables.h record_mgr.h
	$(CC) $(CFLAGS) -c $<

buffer_mgr_stat.o: buffer_mgr_stat.c buffer_mgr_stat.h buffer_mgr.h
	$(CC) $(CFLAGS) -c $<

buffer_mgr.o: buffer_mgr.c buffer_mgr.h dt.h storage_mgr.h
	$(CC) $(CFLAGS) -c $<

storage_mgr.o: storage_mgr.c storage_mgr.h
	$(CC) $(CFLAGS) -c $<

dberror.o: dberror.c dberror.h
	$(CC) $(CFLAGS) -c $<

# ===== Utilities =====
clean:
	$(RM) recordmgr test_expr smoketest smoke.bin *.o *~

help:
	@echo "Targets:"
	@echo "  make            (build recordmgr)"
	@echo "  make run        (run ./recordmgr)"
	@echo "  make run ARG=foo.bin   (run ./recordmgr with an argument)"
	@echo "  make asan       (rebuild with AddressSanitizer)"
	@echo "  make asan-run   (asan build + run)"
	@echo "  make lldb       (launch LLDB on ./recordmgr)"
	@echo "  make run_expr   (run test_expr)"

# ===== Run targets =====
run: recordmgr
	./recordmgr

# If your program expects a file argument (argv[1]), use:
run_arg: recordmgr
	./recordmgr $(ARG)

run_expr: test_expr
	./test_expr

# ===== Debug helpers =====
asan:
	$(MAKE) clean
	$(MAKE) SAN=asan

asan-run:
	$(MAKE) asan
	./recordmgr $(ARG)

lldb: recordmgr
	lldb ./recordmgr

