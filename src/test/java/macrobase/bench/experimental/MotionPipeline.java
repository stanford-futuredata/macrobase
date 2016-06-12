package macrobase.bench.experimental;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import javafx.scene.paint.Stop;
import macrobase.analysis.classify.BatchingPercentileClassifier;
import macrobase.analysis.classify.DumpClassifier;
import macrobase.analysis.classify.OutlierClassifier;
import macrobase.analysis.pipeline.BasePipeline;
import macrobase.analysis.pipeline.Pipeline;
import macrobase.analysis.pipeline.operator.MBGroupBy;
import macrobase.analysis.pipeline.operator.MBOperator;
import macrobase.analysis.pipeline.operator.MBProducer;
import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.analysis.summary.BatchSummarizer;
import macrobase.analysis.summary.Summarizer;
import macrobase.analysis.summary.Summary;
import macrobase.analysis.transform.BatchScoreFeatureTransform;
import macrobase.analysis.transform.FeatureTransform;
import macrobase.analysis.transform.LinearMetricNormalizer;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.DataIngester;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.bytedeco.javacpp.indexer.DoubleIndexer;
import org.bytedeco.javacpp.indexer.FloatIndexer;
import org.bytedeco.javacpp.opencv_core.*;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.Java2DFrameConverter;
import org.bytedeco.javacv.OpenCVFrameConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.bytedeco.javacpp.opencv_core.*;
import static org.bytedeco.javacpp.opencv_highgui.*;
import static org.bytedeco.javacpp.opencv_imgproc.*;
import static org.bytedeco.javacpp.opencv_core.*;
import static org.bytedeco.javacpp.opencv_imgcodecs.*;
import static org.bytedeco.javacpp.opencv_video.*;
import static org.bytedeco.javacpp.opencv_imgproc.*;
import static org.bytedeco.javacpp.opencv_objdetect.*;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.io.File;
import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MotionPipeline extends BasePipeline {
    private static final Logger log = LoggerFactory.getLogger(MotionPipeline.class);

    @Override
    public Pipeline initialize(MacroBaseConf conf) throws Exception {
        super.initialize(conf);
        conf.sanityCheckBatch();
        return this;
    }

    private static final double POINTS_PER_SEC = 5;
    private static final double SCALEDOWN_FACTOR = 10;
    private static final double PERIOD_LENGTH_SEC = 4;

    private class CVDatum extends Datum {
        public Mat videoMat;

        public CVDatum(List<Integer> attrs, Mat videoMat) {
            super(attrs, 0);
            this.videoMat = videoMat;
        }
    }

    private class OpticalFlowFeatureTransform extends FeatureTransform {
        private MBStream<Datum> output = new MBStream<>();

        @Override
        public void initialize() throws Exception {

        }

        @Override
        public void consume(List<Datum> records) throws Exception {

            Mat prevMat = null;

            for(Datum dd : records) {
                CVDatum cvd = (CVDatum) dd;

                if(prevMat == null) {
                    prevMat = cvd.videoMat;
                    continue;
                }


                Mat pGray = prevMat,
                        cGray = cvd.videoMat,
                        Optical_Flow = new Mat();

                RealVector dvec = new ArrayRealVector(cGray.rows()*cGray.cols());

                final DenseOpticalFlow tvl1 = createOptFlow_DualTVL1();
                tvl1.calc(pGray, cGray, Optical_Flow);

                FloatIndexer idx = Optical_Flow.createIndexer();

                final int height = pGray.rows(), width = pGray.cols();

                int pixel = 0;
                for (int y = 0; y < width; y++) {
                    for (int x = 0; x < height; x++) {
                        final float xVelocity = idx.get(x, y);
                        final float yVelocity = idx.get(x, y);
                        final double pixelVelocity = (double) Math
                                .sqrt(xVelocity * xVelocity + yVelocity * yVelocity);
                        dvec.setEntry(pixel, pixelVelocity);
                        pixel += 1;
                    }
                }

                output.add(new Datum(cvd.attributes(), dvec.getNorm()));
            }
        }

        @Override
        public void shutdown() throws Exception {

        }

        @Override
        public MBStream<Datum> getStream() throws Exception {
            return output;
        }
    }

    private class VideoLoader implements MBProducer<Datum> {
        MBStream<Datum> out = new MBStream<>();
        public VideoLoader(String inputDirectory, String framesDirectory) throws Exception{
            File inputFolder = new File(inputDirectory);



            log.info("folder {}, {}", inputFolder, framesDirectory);

            for(File fn : inputFolder.listFiles()) {
                String sourceFile = fn.getName();

                File outputFolder = new File(framesDirectory + "/" + fn.getName().replaceAll(".mpg", ""));
                outputFolder.mkdirs();


                FFmpegFrameGrabber g = new FFmpegFrameGrabber(fn.getAbsoluteFile());
                Java2DFrameConverter fc = new Java2DFrameConverter();

                OpenCVFrameConverter ocv_fc = new OpenCVFrameConverter.ToMat();

                g.start();

                double skip_rate = g.getFrameRate() / POINTS_PER_SEC;
                double nextOutput = skip_rate;
                double frameRate = g.getFrameRate();
                double framesPerPeriod = (PERIOD_LENGTH_SEC * g.getFrameRate());
                log.info("frames per period is {}, skip rate is {}", framesPerPeriod, skip_rate);

                for (int i = 0; i < g.getLengthInFrames(); i++) {
                    Frame f = g.grab();

                    if (i > nextOutput) {
                        int seconds_elapsed = (int) (i / frameRate);

                        BufferedImage bi = fc.convert(f);

                        if(bi == null) {
                            break;
                        }

                        int period = (int) (i / framesPerPeriod);
                        String frameFilename = outputFolder.getAbsolutePath() + "/" + sourceFile
                                                + "-frame-" + String.valueOf(i)
                                                + "-period-" + String.valueOf(period)
                                                + "-seconds-" + String.valueOf(seconds_elapsed)
                                                + ".png";

                        ImageIO.write(bi, "png",
                                      new File(frameFilename));

                        // cvConvert doesn't seem to work, so just reload it as grayscale...
                        IplImage ipImg = cvLoadImage(frameFilename, CV_LOAD_IMAGE_GRAYSCALE);
                        Mat m = ocv_fc.convertToMat(ocv_fc.convert(ipImg));


                        Mat downScaled = new Mat((int) (m.rows() / SCALEDOWN_FACTOR),
                                                 (int) (m.cols() / SCALEDOWN_FACTOR), CV_8UC1);

                        // these bindings are so messed up...
                        pyrDown(m, downScaled);
                        Mat downScaled2 = new Mat();
                        pyrDown(downScaled, downScaled2);

                        conf.getEncoder().recordAttributeName(0, "video_filename");
                        int encodedFileName = conf.getEncoder().getIntegerEncoding(1, sourceFile);
                        conf.getEncoder().recordAttributeName(1, "period");
                        int encodedTime = conf.getEncoder().getIntegerEncoding(1,
                                                                               String.format("period-%d-start-sec-%d",
                                                                                             period,
                                                                                             (int) (period * PERIOD_LENGTH_SEC)));

                        out.add(new CVDatum(Lists.newArrayList(encodedFileName, encodedTime), downScaled2));

                        nextOutput += skip_rate;
                    }
                }

                g.stop();
            }
        }

        @Override
        public MBStream<Datum> getStream() throws Exception {
            return out;
        }
    }


    @Override
    public List<AnalysisResult> run() throws Exception {
        Stopwatch sw = Stopwatch.createStarted();

        VideoLoader v = new VideoLoader(System.getProperty("user.dir")+"/source_vids",
                                        System.getProperty("user.dir")+"/frame_dump");

        System.gc();
        final long loadMs = sw.elapsed(TimeUnit.MILLISECONDS);

        Summarizer s = new BatchSummarizer(conf);

        Stopwatch flowTimer = Stopwatch.createStarted();
        MBGroupBy gb = new MBGroupBy(Lists.newArrayList(0), () -> new OpticalFlowFeatureTransform());
        gb.consume(v.getStream().drain());
        long opticalFlowTime = flowTimer.elapsed(TimeUnit.MILLISECONDS);


        Stopwatch restTimer = Stopwatch.createStarted();
        FeatureTransform score = new BatchScoreFeatureTransform(conf, MacroBaseConf.TransformType.MAD);
        score.consume(gb.getStream().drain());
        OutlierClassifier classifier = new BatchingPercentileClassifier(conf);
        classifier.consume(score.getStream().drain());
        s.consume(classifier.getStream().drain());
        Summary result = s.summarize().getStream().drain().get(0);

        final long totalMs = sw.elapsed(TimeUnit.MILLISECONDS) - loadMs;
        final long summarizeMs = result.getCreationTimeMs();
        final long executeMs = totalMs - result.getCreationTimeMs();

        log.info("optical flow took {}, rest took {}", opticalFlowTime, restTimer.elapsed(TimeUnit.MILLISECONDS));
        log.info("took {}ms ({} tuples/sec)",
                 totalMs,
                 (result.getNumInliers() + result.getNumOutliers()) / (double) totalMs * 1000);

        return Arrays.asList(new AnalysisResult(result.getNumOutliers(),
                                                result.getNumInliers(),
                                                loadMs,
                                                executeMs,
                                                summarizeMs,
                                                result.getItemsets()));
    }
}