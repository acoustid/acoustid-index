#include <cstdlib>
#include <sys/time.h>

class Timer
{
    timeval tv[2];

  public:

    void start()
    {
        gettimeofday(&tv[0], NULL);
    }

    void stop()
    {
        gettimeofday(&tv[1], NULL);
	}

    double duration() const
    {
		unsigned long a = tv[0].tv_sec * 1000 * 1000 + tv[0].tv_usec;
		unsigned long b = tv[1].tv_sec * 1000 * 1000 + tv[1].tv_usec;
		return (b - a) / 1000.0;
    }
};
