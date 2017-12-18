CXXFLAGS=-std=c++11 -Wall -pedantic
examples = example1 example2
all: $(examples)

$(examples): %: %.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

.PHONY: clean
clean:
	rm -f $(examples)
