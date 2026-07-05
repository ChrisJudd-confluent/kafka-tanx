TARGET = kafkatanx
SRCS   = kafkatanx.cpp kafka_net.cpp avro_codec.cpp
DEPS   = $(SRCS) kafka_net.h avro_codec.h olcPixelGameEngine.h miniaudio.h

# =============================================================================
# Platform detection
# Windows: run from MSYS2 UCRT64 shell — $(OS) is set to Windows_NT
# macOS:   detected via uname -s == Darwin
# Linux:   detected via uname -s == Linux
#
# Extra dependency vs base tanx: librdkafka (Kafka C++ client)
#   macOS:   brew install librdkafka
#   Linux:   sudo apt install librdkafka-dev
#   Windows: pacman -S mingw-w64-ucrt-x86_64-librdkafka  (in UCRT64 shell)
# =============================================================================

ifeq ($(OS),Windows_NT)
    CXX      = g++
    CXXFLAGS = -std=c++17 -Wall -O2
    LDFLAGS  = -static-libgcc -static-libstdc++ \
               -Wl,-Bstatic -lpthread -Wl,-Bdynamic \
               -lrdkafka++ -lrdkafka \
               -luser32 -lgdi32 -lopengl32 -lgdiplus -lshlwapi -ldwmapi \
               -lstdc++fs -lws2_32
    TARGET  := kafkatanx.exe
else
    UNAME_S := $(shell uname -s)

    ifeq ($(UNAME_S),Darwin)
        LIBPNG_PREFIX     := $(shell brew --prefix libpng 2>/dev/null)
        LIBRDKAFKA_PREFIX := $(shell brew --prefix librdkafka 2>/dev/null)
        CXX      = clang++
        CXXFLAGS = -std=c++17 -Wall -O2 -mmacosx-version-min=10.15 \
                   -Wno-deprecated-declarations \
                   -I$(LIBPNG_PREFIX)/include \
                   -I$(LIBRDKAFKA_PREFIX)/include
        LDFLAGS  = -framework OpenGL -framework GLUT -framework Carbon \
                   -L$(LIBPNG_PREFIX)/lib \
                   -L$(LIBRDKAFKA_PREFIX)/lib
        LIBS     = -lpng -lrdkafka++
    endif

    ifeq ($(UNAME_S),Linux)
        CXX      = g++
        CXXFLAGS = -std=c++17 -Wall -O2
        LDFLAGS  = -lX11 -lGL -lpthread -ldl -lstdc++fs -lrdkafka++
        LIBS     = -lpng
    endif
endif

all: $(TARGET)

$(TARGET): $(DEPS)
	$(CXX) $(CXXFLAGS) $(SRCS) -o $(TARGET) $(LDFLAGS) $(LIBS)

clean:
	rm -f kafkatanx kafkatanx.exe

run: $(TARGET)
	./$(TARGET)

.PHONY: all clean run
