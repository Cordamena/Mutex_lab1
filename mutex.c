#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>

typedef struct {
    pthread_mutex_t mtx;
    pthread_cond_t  cond;
    int             has_event; 
    int             done;       
    void           *event_ptr;
} monitor_t;

static monitor_t mon = {
    .mtx = PTHREAD_MUTEX_INITIALIZER,
    .cond = PTHREAD_COND_INITIALIZER,
    .has_event = 0,
    .done = 0,
    .event_ptr = NULL
};

#define NUM_PACKETS 7  

static void *producer(void *arg)
{
    (void)arg;
    for (int i = 1; i <= NUM_PACKETS; ++i) {
        sleep(1);

        int *payload = malloc(sizeof(int));
        if (!payload) { perror("malloc"); exit(EXIT_FAILURE); }
        *payload = i * 100;

        pthread_mutex_lock(&mon.mtx);
        while (mon.has_event)
            pthread_cond_wait(&mon.cond, &mon.mtx);
        mon.event_ptr = payload;
        mon.has_event = 1;
        printf("Provider has sent a payload #%d (payload=%d)\n",
               i, *payload);
        fflush(stdout);
        pthread_cond_signal(&mon.cond);
        pthread_mutex_unlock(&mon.mtx);
    }

    pthread_mutex_lock(&mon.mtx);
    mon.done = 1;
    printf("Provider has stopped the transmission.\n");
    fflush(stdout);
    pthread_cond_signal(&mon.cond);
    pthread_mutex_unlock(&mon.mtx);

    return NULL;
}

static void *consumer(void *arg)
{
    (void)arg;
    for (;;) {
        pthread_mutex_lock(&mon.mtx);
        while (!mon.has_event && !mon.done)
            pthread_cond_wait(&mon.cond, &mon.mtx);

        if (!mon.has_event && mon.done) {
            pthread_mutex_unlock(&mon.mtx);
            break;
        }

        int *payload = mon.event_ptr;
        mon.event_ptr = NULL;
        mon.has_event = 0;

        printf("Consumer has received a payload: (payload=%d)\n", *payload);
        fflush(stdout);

        pthread_cond_signal(&mon.cond);
        pthread_mutex_unlock(&mon.mtx);

        free(payload);
    }

    printf("Transmission terminated. Consumer out.\n");
    fflush(stdout);
    return NULL;
}

int main(void)
{
    pthread_t tp, tc;

    if (pthread_create(&tp, NULL, producer, NULL) != 0) {
        perror("pthread_create producer"); exit(EXIT_FAILURE);
    }
    if (pthread_create(&tc, NULL, consumer, NULL) != 0) {
        perror("pthread_create consumer"); exit(EXIT_FAILURE);
    }

    pthread_join(tp, NULL);
    pthread_join(tc, NULL);

    pthread_mutex_destroy(&mon.mtx);
    pthread_cond_destroy(&mon.cond);
    return 0;
}