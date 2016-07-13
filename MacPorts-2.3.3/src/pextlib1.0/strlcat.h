/* $Id: strlcat.h 108069 2013-07-12 05:28:37Z jmr@macports.org $ */
#ifdef HAVE_STRLCAT
#include <string.h>
#else
size_t strlcat(char *dst, const char *src, size_t size);
#endif
