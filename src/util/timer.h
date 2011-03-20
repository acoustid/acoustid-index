// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_UTIL_TIMER_H_
#define ACOUSTID_UTIL_TIMER_H_

#include "common.h"
#include <sys/time.h>

class Timer
{
public:

    void start()
    {
		m_started = currentTime();
    }

    double restart()
    {
		unsigned long now = currentTime();
		double elapsed = (now - m_started) / 1000.0;
		m_started = now;
		return elapsed;
	}

    double elapsed() const
    {
		return (currentTime() - m_started) / 1000.0;
    }

private:

	unsigned long currentTime() const
	{
		struct timeval tv;
        gettimeofday(&tv, NULL);
		return tv.tv_sec * 1000 * 1000 + tv.tv_usec;
	}

	unsigned long m_started;
};

#endif

