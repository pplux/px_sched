CXXFLAGS=-std=c++11 -Wall -pedantic

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
  CXXCFLAGS += -lpthread
endif

examples = example1 example2 example3 example4 example5 example6
all: $(examples)

$(examples): %: %.cpp ../px_sched.h
	$(CXX) $(CXXFLAGS) -o $@ $<

.PHONY: clean
clean:
	rm -f $(examples)
