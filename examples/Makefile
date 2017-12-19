CXXFLAGS=-std=c++11 -Wall -pedantic -pthread
LDFLAGS = -lpthread
examples = example1 example2 example3 example4 example5 example6
all: $(examples)

$(examples): %: %.cpp
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $<

.PHONY: clean
clean:
	rm -f $(examples)
