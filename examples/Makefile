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
	-Wmissing-declarations \
	-Wmissing-include-dirs \
	-Wno-unused\
	-Wnull-dereference -pedantic \
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

.PHONY: clean
clean:
	rm -f $(examples)
