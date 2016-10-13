#ifndef __LOG__
#define __LOG__


#define LOG_DEBUG(msg, ...)			printf("[DEBUG]: " msg, ##__VA_ARGS__)
#define LOG_INFO(msg, ...)			printf("[INFO]: "  msg, ##__VA_ARGS__)
#define LOG_WARN(msg, ...)			printf("[WARN]: "  msg, ##__VA_ARGS__)
#define LOG_ERROR(msg, ...)			printf("[ERROR]: " msg, ##__VA_ARGS__)
#define LOG_FATAL(msg, ...)			printf("[FATAL]: " msg, ##__VA_ARGS__)

#endif
