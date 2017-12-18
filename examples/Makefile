CXXFLAGS=-std=c++11 -Wall -pedantic
examples = example1 example2 example3 example4 example5 example6
all: $(examples)

$(examples): %: %.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

.PHONY: clean
clean:
	rm -f $(examples)
