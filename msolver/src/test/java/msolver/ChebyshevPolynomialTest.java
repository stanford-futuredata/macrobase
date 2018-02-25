package msolver;

import msolver.chebyshev.ChebyshevPolynomial;
import msolver.chebyshev.QuadraticCosFunction;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ChebyshevPolynomialTest {
    @Test
    public void testSimple() {
        double[] coeff = {1.0, 2.0, 3.0};
        ChebyshevPolynomial cp = new ChebyshevPolynomial(coeff);
        assertEquals(2.34, cp.value(.7), 1e-10);
        assertEquals(cp.value(.7), cp.value2(.7), 1e-10);

        ChebyshevPolynomial cb = ChebyshevPolynomial.basis(2);
        assertEquals(-0.02, cb.value(.7), 1e-10);

        double[] coeff2 = {2.0, 1.0, 3.0};
        cp = new ChebyshevPolynomial(coeff2);
        assertEquals(2.0, cp.integrate(), 1e-10);

        assertEquals(1.848, cp.multiplyByBasis(1).value(.7), 1e-10);
    }

    @Test
    public void testFitMulti() {
        QuadraticCosFunction multiFunction = new QuadraticCosFunction(3);
        ChebyshevPolynomial[] cfit = ChebyshevPolynomial.fitMulti(multiFunction, 1e-10);
        for (int i = 0; i < cfit.length; i++) {
            assertEquals((i+1)*.25, cfit[i].value(.5), 1e-10);
        }
    }

    @Test
    public void testFit() {
        double[] coeff = {1.0, 2.0, 3.0};
        ChebyshevPolynomial cp = new ChebyshevPolynomial(coeff);
        ChebyshevPolynomial cfit = ChebyshevPolynomial.fit(cp, 1e-10);

        for (int i = 0; i < coeff.length; i++) {
            assertEquals(coeff[i], cfit.coeffs()[i], 1e-10);
        }
    }

    @Test
    public void testIntegrate() {
        double[] coeff = {2.0, 1.0, 3.0};
        ChebyshevPolynomial cp = new ChebyshevPolynomial(coeff);
        assertEquals(cp.integrate(), cp.integralPoly().value(1), 1e-10);
    }

    @Test
    public void testMultiply() {
        double[] c1 = {1.0, 2.0, 3.0, 4.0};
        double[] c2 = {.5, .6, .7, .8, .9};
        ChebyshevPolynomial cp1 = new ChebyshevPolynomial(c1);
        ChebyshevPolynomial cp2 = new ChebyshevPolynomial(c2);

        ChebyshevPolynomial product = cp1.multiply(cp2);
        assertEquals(
                cp1.value(.5)*cp2.value(.5),
                product.value(.5),
                1e-10
                );

        product = cp1.multiply(cp1);
        assertEquals(
                cp1.value(.7)*cp1.value(.7),
                product.value(.7),
                1e-10
        );
    }
}