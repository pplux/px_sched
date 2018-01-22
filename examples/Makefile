CXXFLAGS=-std=c++11 -pedantic \
	-Wall \
	-Wcast-align \
	-Wcast-qual \
	-Wctor-dtor-privacy \
	-Wdisabled-optimization \
	-Wdouble-promotion \
	-Werror \
	-Wextra \
	-Wformat=2 \
	-Winit-self \
	-Wmissing-include-dirs \
	-Wno-unused\
	-Wold-style-cast \
	-Woverloaded-virtual \
	-Wredundant-decls \
	-Wshadow \
	-Wsign-conversion \
	-Wsign-promo \
	-Wstrict-overflow=5 \
	-Wswitch-default \
	-Wundef \

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
  LDFLAGS += -lpthread
endif

examples = example1 example2 example3 example4 example5 example6
all: $(examples)

$(examples): %: %.cpp ../px_sched.h
	$(CXX) $(CXXFLAGS) -o $@ $(LDFLAGS) $<
	$(CXX) $(CXXFLAGS) -DPX_SCHED_CHECK_DEADLOCKS=1 -o $@_deadlock_check $(LDFLAGS) $<


.PHONY: clean tests
clean:
	rm -f $(examples)

tests: $(examples)
	./example1 
	./example2 
	./example3 
	./example4 
	./example5 
	./example6 
	./example1_deadlock_check 
	./example2_deadlock_check 
	./example3_deadlock_check 
	./example4_deadlock_check 
	./example5_deadlock_check 
	./example6_deadlock_check 
	@echo "ALL examples executed"
