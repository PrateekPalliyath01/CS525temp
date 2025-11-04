# Compiler and flags
CC = gcc
CFLAGS = -g -Wall -std=c99
LIBS = -lm

# Source files
STORAGE_SRC = storage_mgr.c
BUFFER_SRC = buffer_mgr.c buffer_mgr_stat.c
RECORD_SRC = record_mgr.c rm_serializer.c expr.c
COMMON_SRC = dberror.c

# Test files
TEST_EXPR = test_expr.c
TEST_ASSIGN3 = test_assign3_1.c
TEST_SIMPLE = test_simple.c

# Object files
STORAGE_OBJ = storage_mgr.o
BUFFER_OBJ = buffer_mgr.o buffer_mgr_stat.o
RECORD_OBJ = record_mgr.o rm_serializer.o expr.o
COMMON_OBJ = dberror.o
TEST_EXPR_OBJ = test_expr.o
TEST_ASSIGN3_OBJ = test_assign3_1.o
TEST_SIMPLE_OBJ = test_simple.o

# Executables
TEST_EXPR_EXEC = test_expr
TEST_ASSIGN3_EXEC = test_assign3
TEST_SIMPLE_EXEC = test_simple

# Default target
all: $(TEST_EXPR_EXEC) $(TEST_ASSIGN3_EXEC) $(TEST_SIMPLE_EXEC)

# Build test_expr executable
$(TEST_EXPR_EXEC): $(TEST_EXPR_OBJ) $(RECORD_OBJ) $(COMMON_OBJ) $(STORAGE_OBJ) $(BUFFER_OBJ)
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

# Build test_assign3 executable
$(TEST_ASSIGN3_EXEC): $(TEST_ASSIGN3_OBJ) $(RECORD_OBJ) $(COMMON_OBJ) $(STORAGE_OBJ) $(BUFFER_OBJ)
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

# Build test_simple executable
$(TEST_SIMPLE_EXEC): $(TEST_SIMPLE_OBJ) $(RECORD_OBJ) $(COMMON_OBJ) $(STORAGE_OBJ) $(BUFFER_OBJ)
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

# Compile object files
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

# Run test_assign3
run: $(TEST_ASSIGN3_EXEC)
	./$(TEST_ASSIGN3_EXEC)

# Run test_expr
run_expr: $(TEST_EXPR_EXEC)
	./$(TEST_EXPR_EXEC)

# Run test_simple
run_simple: $(TEST_SIMPLE_EXEC)
	./$(TEST_SIMPLE_EXEC)

# Clean build files
clean:
	rm -f *.o $(TEST_EXPR_EXEC) $(TEST_ASSIGN3_EXEC) $(TEST_SIMPLE_EXEC)

# Phony targets
.PHONY: all clean run run_expr run_simple
