package macrobase.analysis.stats.kde.kernel;

import java.util.function.Supplier;

/**
 * Generate kernel constructors given kernel type
 */
public class KernelFactory {
    public Supplier<Kernel> supplier;

    public KernelFactory(String kernel) {
        if (kernel.equals("gaussian")) {
            supplier = GaussianKernel::new;
        } else {
            supplier = EpaKernel::new;
        }
    }

    public Kernel get() {
        return supplier.get();
    }
}
