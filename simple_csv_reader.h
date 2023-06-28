
#include <stdint.h>

#include "adbc.h"

#ifdef __cplusplus
extern "C" {
#endif

void InitSimpleCsvArrayStream(const char* filename, int64_t filename_size,
                              ArrowArrayStream* out);

#ifdef __cplusplus
}
#endif
