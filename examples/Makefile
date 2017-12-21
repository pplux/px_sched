CXXFLAGS=-std=c++11 -Wall -pedantic -pthread

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
  LDFLAGS += -lpthread
endif

examples = example1 example2 example3 example4 example5 example6
all: $(examples)

$(examples): %: %.cpp ../px_sched.h
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $<

.PHONY: clean
clean:
	rm -f $(examples)
