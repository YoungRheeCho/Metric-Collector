#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>

int main(int argc, char* argv[]) {
    const char* config_path = NULL;

    static struct option long_options[] = {
        {"config", required_argument, NULL, 'c'},
        {"help",   no_argument,       NULL, 'h'},
        {NULL, 0, NULL, 0}
    };

    int opt;

    while ((opt = getopt_long(argc, argv, "c:h", long_options, NULL)) != -1) {
        switch (opt) {
            case 'c':
                config_path = optarg;
                break;
            case 'h':
                printf("help code\n", argv[0]);
                return 0;
            default:
                fprintf(stderr, "command: %s -c <config file>\n", argv[0]);
                return 1;
        }
    }

    if (config_path == NULL) {
        fprintf(stderr, "No config(-c)\n");
        return 1;
    }

    //printf("config file: %s\n", config_path);

    /* 
        1. config load
        2. config 기반 인스턴스 생성
    */
    return 0;
}