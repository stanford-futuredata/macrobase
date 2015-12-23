package macrobase.analysis.sample;

import org.joda.time.DateTime;
import org.joda.time.Period;

import java.util.Collection;

/*
 Exponentially weighted time-decaying reservoir sample with period and
 bias per period specified at construction.
 */
public class TimeDecayingAChao<T> {
    private final Period period;
    private final long periodMs;
    private long previousPeriodAdvance = DateTime.now().getMillis();
    private ExponentiallyBiasedAChao<T> _chao;

    public TimeDecayingAChao(int capacity, double bias, Period period) {
        _chao = new ExponentiallyBiasedAChao<>(capacity, bias);

        assert(period.getMillis() > 0);

        this.period = period;
        periodMs = this.period.getMillis();
    }

    private void checkAndAdvancePeriod() {
        // working in ms is kind of ugly, but Joda doesn't have a nice way to
        // divide a TimeUnit by a Period, only to multiply a Period by a scalar
        long nowMs = DateTime.now().getMillis();
        if(previousPeriodAdvance + periodMs < nowMs) {
            int missingPeriods = (int)(((double)nowMs - previousPeriodAdvance)/ periodMs);
            assert(missingPeriods > 0);
            _chao.advancePeriod(missingPeriods);

            previousPeriodAdvance += missingPeriods* periodMs;
        }
    }

    // amortize the overhead of time checking
    public void insertBatch(Collection<T> elements) {
        checkAndAdvancePeriod();

        for(T ele : elements) {
            _chao.insert(ele);
        }
    }

    public void insert(T ele) {
        checkAndAdvancePeriod();
        _chao.insert(ele);
    }
}
