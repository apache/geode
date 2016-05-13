#include <gf_client.h>
#include <stdio.h>

void printByteArray(int8_t* arr, int32_t len)
{
    int32_t i = 0;
    for(i = 0; i < len; i++)
    {
        printf("%d", arr[i]);
    }
}


int main(int argc, char** argv)
{
    int resultcode = NO_ERROR;
    char* key = "key";
    int8_t data[] = {1,2,3,4};
    int8_t returnData[4];
    CONTEXT* context = gf_connect("localhost", "40404", &resultcode);
    if(resultcode == NO_ERROR)
    {
        printf("Connection successful.\n");
        if(context != NULL) {
            resultcode = NO_ERROR;
            printf("Sending ping message... ");
            gf_ping(context, &resultcode);
            if(resultcode == NO_ERROR)
            {
                printf("successful.\n");
            } else {
                printf("failed. Error code: %d\n", resultcode);
            }

            resultcode = NO_ERROR;
            printf("Sending put with key=%s, and value=", key);
            printByteArray(data, 4);
            printf("... ");
            gf_put(context, key, data, 4, &resultcode);
            if(resultcode == NO_ERROR)
            {
                printf("successful.\n");
            } else {
                printf("failed. Error code: %d\n", resultcode);
            }

            resultcode = NO_ERROR;
            printf("Sending get message with key=%s... ", key);
            gf_get(context, "key",returnData, 1024, &resultcode);
            if(resultcode == NO_ERROR)
            {
                printf("successful. got '");
                printByteArray(returnData, 4);
                printf("'.\n");
            } else {
                printf("failed. Error code: %d\n", resultcode);
            }

            resultcode = NO_ERROR;
            printf("Sending destroy with key=%s... ", key);
            gf_destroy(context, "key", &resultcode);
            if(resultcode == NO_ERROR)
            {
                printf("successful.\n");
            } else {
                printf("failed. Error code: %d\n", resultcode);
            }

            resultcode = NO_ERROR;
            printf("disconnecting... ");
            gf_disconnect(context, &resultcode);
            if(resultcode == NO_ERROR)
            {
                printf("successful.\n");
            } else {
                printf("failed. Error code: %d\n", resultcode);
            }
        }
    } else {
        printf("Connection failure. Error Code: %d\n", resultcode);
    }

    return 0;
}

