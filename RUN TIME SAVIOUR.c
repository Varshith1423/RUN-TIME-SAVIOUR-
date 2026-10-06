#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdarg.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <semaphore.h>
#include <sys/neutrino.h>
#include <sys/iofunc.h>
#include <sys/dispatch.h>

#define NUM_SECTIONS   4
#define CTRL_NAME      "railway_ctrl"
#define DWELL_MS       800
#define RETRY_MS       300
#define LOG_Q          128
#define LOG_LEN        200

#define MSG_ENTER      (_IO_MAX + 1)
#define MSG_EXIT       (_IO_MAX + 2)
#define MSG_EMERGENCY  (_IO_MAX + 3)
#define MSG_CLEAR      (_IO_MAX + 4)
#define MSG_SHUTDOWN   (_IO_MAX + 5)

enum { ST_OK = 0, ST_DENIED_OCCUPIED = 1, ST_DENIED_EMERGENCY = 2 };
enum { SIG_RED = 0, SIG_YELLOW = 1, SIG_GREEN = 2 };

typedef struct { uint16_t type; int train; int section; } msg_t;
typedef struct { int status; } reply_t;
typedef union { uint16_t type; struct _pulse pulse; msg_t m; } recv_t;

typedef struct { int id; int delay_ms; } train_cfg_t;

static pthread_mutex_t state_mtx = PTHREAD_MUTEX_INITIALIZER;
static int occupied[NUM_SECTIONS];
static int signal_state[NUM_SECTIONS];
static int emergency_active;

static char logq[LOG_Q][LOG_LEN];
static int lq_head, lq_tail, lq_count, logger_stop;
static pthread_mutex_t lq_mtx = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t lq_cv = PTHREAD_COND_INITIALIZER;

static sem_t ready_sem;
static struct timespec t0;

static long now_ms(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (t.tv_sec - t0.tv_sec) * 1000L +
           (t.tv_nsec - t0.tv_nsec) / 1000000L;
}

static void msleep(int ms)
{
    usleep((useconds_t)ms * 1000);
}

static void log_event(const char *tag, const char *fmt, ...)
{
    char body[150];
    va_list ap;

    va_start(ap, fmt);
    vsnprintf(body, sizeof body, fmt, ap);
    va_end(ap);

    pthread_mutex_lock(&lq_mtx);

    if (lq_count < LOG_Q) {
        snprintf(
            logq[lq_head],
            LOG_LEN,
            "[%5ld ms] %-10s %s",
            now_ms(),
            tag,
            body
        );

        lq_head = (lq_head + 1) % LOG_Q;
        lq_count++;
        pthread_cond_signal(&lq_cv);
    }

    pthread_mutex_unlock(&lq_mtx);
}

static void *logger_thread(void *arg)
{
    (void)arg;

    FILE *fp = fopen("railway_log.txt", "w");
    char line[LOG_LEN];

    for (;;) {
        pthread_mutex_lock(&lq_mtx);

        while (lq_count == 0 && !logger_stop)
            pthread_cond_wait(&lq_cv, &lq_mtx);

        if (lq_count == 0 && logger_stop) {
            pthread_mutex_unlock(&lq_mtx);
            break;
        }

        strncpy(line, logq[lq_tail], LOG_LEN);
        lq_tail = (lq_tail + 1) % LOG_Q;
        lq_count--;

        pthread_mutex_unlock(&lq_mtx);

        puts(line);
        fflush(stdout);

        if (fp) {
            fprintf(fp, "%s\n", line);
            fflush(fp);
        }
    }

    if (fp)
        fclose(fp);

    return NULL;
}

static const char *sig_name(int s)
{
    return s == SIG_RED ? "RED" :
           (s == SIG_YELLOW ? "YELLOW" : "GREEN");
}

static void update_signals(void)
{
    for (int i = 0; i < NUM_SECTIONS; i++) {
        if (emergency_active || occupied[i])
            signal_state[i] = SIG_RED;
        else if (i + 1 < NUM_SECTIONS && occupied[i + 1])
            signal_state[i] = SIG_YELLOW;
        else
            signal_state[i] = SIG_GREEN;
    }
}

static void board(char *out, size_t n)
{
    size_t off = 0;

    off += snprintf(out + off, n - off, "TRACK ");

    for (int i = 0; i < NUM_SECTIONS && off < n; i++) {
        if (occupied[i])
            off += snprintf(
                out + off,
                n - off,
                "S%d[T%d] ",
                i,
                occupied[i]
            );
        else
            off += snprintf(
                out + off,
                n - off,
                "S%d[--] ",
                i
            );
    }

    off += snprintf(out + off, n - off, "| SIGNALS ");

    for (int i = 0; i < NUM_SECTIONS && off < n; i++)
        off += snprintf(
            out + off,
            n - off,
            "S%d=%s ",
            i,
            sig_name(signal_state[i])
        );
}

static void *controller_thread(void *arg)
{
    (void)arg;

    name_attach_t *att = name_attach(NULL, CTRL_NAME, 0);

    if (att == NULL) {
        perror("name_attach");
        exit(EXIT_FAILURE);
    }

    pthread_mutex_lock(&state_mtx);
    update_signals();
    pthread_mutex_unlock(&state_mtx);

    log_event(
        "CONTROLLER",
        "started, %d track sections, all signals initialised",
        NUM_SECTIONS
    );

    sem_post(&ready_sem);

    int running = 1;

    while (running) {
        recv_t msg;

        int rcvid = MsgReceive(
            att->chid,
            &msg,
            sizeof msg,
            NULL
        );

        if (rcvid == -1) {
            if (errno == EINTR)
                continue;
            break;
        }

        if (rcvid == 0) {
            if (msg.pulse.code == _PULSE_CODE_DISCONNECT)
                ConnectDetach(msg.pulse.scoid);

            continue;
        }

        if (msg.type == _IO_CONNECT) {
            MsgReply(rcvid, EOK, NULL, 0);
            continue;
        }

        if (msg.type > _IO_BASE && msg.type <= _IO_MAX) {
            MsgError(rcvid, ENOSYS);
            continue;
        }

        reply_t rep = { ST_OK };
        char brd[160];
        int changed = 0;

        pthread_mutex_lock(&state_mtx);

        switch (msg.m.type) {
            case MSG_ENTER: {
                int s = msg.m.section;
                int t = msg.m.train;

                if (emergency_active) {
                    rep.status = ST_DENIED_EMERGENCY;

                    log_event(
                        "DETECT",
                        "Train %d -> S%d DENIED (emergency active)",
                        t,
                        s
                    );
                }
                else if (occupied[s]) {
                    rep.status = ST_DENIED_OCCUPIED;

                    log_event(
                        "DETECT",
                        "Train %d -> S%d DENIED (occupied by T%d)",
                        t,
                        s,
                        occupied[s]
                    );
                }
                else {
                    occupied[s] = t;
                    update_signals();
                    changed = 1;

                    log_event(
                        "DETECT",
                        "Train %d entered S%d",
                        t,
                        s
                    );
                }

                break;
            }

            case MSG_EXIT:
                log_event(
                    "DETECT",
                    "Train %d cleared S%d",
                    msg.m.train,
                    msg.m.section
                );

                occupied[msg.m.section] = 0;
                update_signals();
                changed = 1;

                break;

            case MSG_EMERGENCY:
                emergency_active = 1;
                update_signals();
                changed = 1;

                log_event(
                    "EMERGENCY",
                    "!!! EMERGENCY STOP - all signals forced RED"
                );

                break;

            case MSG_CLEAR:
                emergency_active = 0;
                update_signals();
                changed = 1;

                log_event(
                    "EMERGENCY",
                    "Emergency cleared - normal operation resumed"
                );

                break;

            case MSG_SHUTDOWN:
                running = 0;
                break;
        }

        if (changed)
            board(brd, sizeof brd);

        pthread_mutex_unlock(&state_mtx);

        if (changed)
            log_event("SIGNAL", "%s", brd);

        MsgReply(rcvid, EOK, &rep, sizeof rep);
    }

    log_event("CONTROLLER", "shutting down");

    name_detach(att, 0);

    return NULL;
}

static int send_msg(int coid, uint16_t type, int train, int section)
{
    msg_t m = { type, train, section };
    reply_t r = { -1 };

    if (MsgSend(coid, &m, sizeof m, &r, sizeof r) == -1) {
        perror("MsgSend");
        return -1;
    }

    return r.status;
}

static void *train_thread(void *arg)
{
    train_cfg_t *t = arg;

    int coid = name_open(CTRL_NAME, 0);

    if (coid == -1) {
        perror("name_open");
        return NULL;
    }

    msleep(t->delay_ms);

    int prev = -1;

    for (int s = 0; s < NUM_SECTIONS; s++) {
        while (send_msg(coid, MSG_ENTER, t->id, s) != ST_OK)
            msleep(RETRY_MS);

        if (prev >= 0)
            send_msg(coid, MSG_EXIT, t->id, prev);

        prev = s;

        msleep(DWELL_MS);
    }

    send_msg(coid, MSG_EXIT, t->id, prev);

    log_event(
        "TRAIN",
        "Train %d completed its route",
        t->id
    );

    name_close(coid);

    return NULL;
}

static void *emergency_thread(void *arg)
{
    (void)arg;

    int coid = name_open(CTRL_NAME, 0);

    if (coid == -1) {
        perror("name_open");
        return NULL;
    }

    msleep(3500);

    send_msg(coid, MSG_EMERGENCY, 0, 0);

    msleep(1500);

    send_msg(coid, MSG_CLEAR, 0, 0);

    name_close(coid);

    return NULL;
}

int main(void)
{
    pthread_t ctrl, logger, emerg, tr[3];

    train_cfg_t cfg[3] = {
        {1, 0},
        {2, 500},
        {3, 2000}
    };

    clock_gettime(CLOCK_MONOTONIC, &t0);

    sem_init(&ready_sem, 0, 0);

    printf(
        "=== Real-Time Railway Signal Control System (QNX simulation) ===\n"
    );

    pthread_create(
        &logger,
        NULL,
        logger_thread,
        NULL
    );

    pthread_create(
        &ctrl,
        NULL,
        controller_thread,
        NULL
    );

    sem_wait(&ready_sem);

    for (int i = 0; i < 3; i++)
        pthread_create(
            &tr[i],
            NULL,
            train_thread,
            &cfg[i]
        );

    pthread_create(
        &emerg,
        NULL,
        emergency_thread,
        NULL
    );

    for (int i = 0; i < 3; i++)
        pthread_join(tr[i], NULL);

    pthread_join(emerg, NULL);

    int coid = name_open(CTRL_NAME, 0);

    if (coid != -1) {
        send_msg(coid, MSG_SHUTDOWN, 0, 0);
        name_close(coid);
    }

    pthread_join(ctrl, NULL);

    pthread_mutex_lock(&lq_mtx);

    logger_stop = 1;
    pthread_cond_signal(&lq_cv);

    pthread_mutex_unlock(&lq_mtx);

    pthread_join(logger, NULL);

    printf(
        "=== Simulation finished. Log saved to railway_log.txt ===\n"
    );

    return 0;
}
