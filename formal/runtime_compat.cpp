#include <unistd.h>
#include <dlfcn.h>
#include <cstdlib>
#include <cstring>
#include <cstdio>
// Runtime compatibility only: this sandbox does not expose /proc/<pid>/exe.
// Tell Lean where its unmodified executable was installed.
extern "C" ssize_t readlink(const char* path, char* buf, size_t size) {
  char own[80]; std::snprintf(own, sizeof own, "/proc/%d/exe", getpid());
  const char* exe = std::getenv("LEAN_EXECUTABLE_PATH");
  if (exe && (!std::strcmp(path, own) || !std::strcmp(path, "/proc/self/exe"))) {
    size_t n = std::strlen(exe); if (n > size) n = size;
    std::memcpy(buf, exe, n); return static_cast<ssize_t>(n);
  }
  auto real_readlink = reinterpret_cast<ssize_t(*)(const char*,char*,size_t)>(dlsym(RTLD_NEXT,"readlink"));
  return real_readlink(path,buf,size);
}
