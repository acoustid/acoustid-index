// Acoustid Index -- Inverted index for audio fingerprints
// Copyright (C) 2011  Lukas Lalinsky
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

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

