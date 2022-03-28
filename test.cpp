#include <iostream>
#include <stdio.h>
#include <cstddef>
int main()
{
    int num = 22;
    char buf[30];
    std::snprintf(buf, sizeof(buf), "%llu", static_cast<unsigned long long>(num));
    std::cout<<buf<<std::endl;
}

