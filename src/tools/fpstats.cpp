#include <stdint.h>
#include <stdio.h>

#include <QDebug>
#include <QFile>
#include <QTextStream>

int main(int argc, char **argv) {
    QTextStream in(stdin);

    unsigned long counts[256];
    memset(counts, 0, sizeof(counts));

    unsigned long long sum = 0;

    int bits = 8;
    bool first = false;

    uint32_t mask, shift;
    if (first) {
        mask = ((1 << bits) - 1);
        shift = 0;
    } else {
        mask = 0xffffffff;
        shift = 32 - bits;
    }

    qDebug() << mask;

    while (!in.atEnd()) {
        uint32_t key, value;
        in >> key >> value;
        if (in.status() != QTextStream::Ok) break;
        counts[(key & mask) >> shift]++;
        sum++;
    }

    unsigned long long sum2 = 0;
    for (int i = 0; i < (1 << bits); i++) {
        qDebug() << i << counts[i] << (100.0 * counts[i] / sum);
        sum2 += counts[i];
    }

    return 0;
}
