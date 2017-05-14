package org.apache.hadoop.hive.ql.io.orc;

import com.google.common.collect.Lists;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

/**
 * SplitGenerator that uses Map/Reduce to generate ORC splits.
 * A Map/Reduce job is launched to examine all selected ORC-files in parallel,
 * and eliminate ORC-stripes based on SARGS evaluation.
 */
public class MapReduceBasedSplitGenerator implements Callable<List<OrcSplit>> {

  private static final Log LOG = LogFactory.getLog(MapReduceBasedSplitGenerator.class);

  protected OrcInputFormat.Context context;
  protected ArrayList<OrcInputFormat.SplitInfo> splitInfos;
  protected UserGroupInformation ugi;

  protected static final String SERIALIZED_SPLIT_INFOS = "hive.exec.orc.split.strategy.etl.mr.impl.serialized.split-gen.splits";
  protected static final String SPLIT_LIST_FILE_NAME = "split.list";

  public MapReduceBasedSplitGenerator(OrcInputFormat.Context context,
      ArrayList<OrcInputFormat.SplitInfo> splitInfos, UserGroupInformation ugi) {

    LOG.debug("Calculating ORC Splits using MapReduce. nSplitInfos == " + splitInfos.size());
    this.context = new OrcInputFormat.Context(new Configuration(context.conf));
    this.splitInfos = splitInfos;
    this.ugi = ugi;
  }

  /**
   * Split class for ORC split-generation job.
   */
  public static final class SplitGeneratorSplit extends    org.apache.hadoop.mapreduce.InputSplit
                                                implements InputSplit, Serializable {

    /**
     * Underlying split implementation. One Impl instance per file to be processed.
     */
    public static class Impl implements Writable, Serializable {

      private String fileName, dirName;
      private Long fileSize;
      private List<String> blockHosts;
      private Boolean isOriginal, hasBase;
      private List<Long> deltas;
      private List<Boolean> covered;

      public Impl() {
        // To be used in split-deserialization.
      }

      public Impl(OrcInputFormat.SplitInfo splitInfo) throws IOException {
        fileName = splitInfo.file.getPath().toString();
        LOG.debug("SplitGeneratorSplit.Impl(" + fileName + ")");
        dirName = splitInfo.dir.toString();
        fileSize = splitInfo.file.getLen();
        // TODO: Smarten up block-host calculation.
        BlockLocation[] locations = OrcInputFormat.SHIMS.getLocations(splitInfo.fs, splitInfo.file);
        blockHosts = Lists.newArrayListWithCapacity(locations.length);
        for (BlockLocation blockLocation : locations) {
          blockHosts.addAll(Arrays.asList(blockLocation.getHosts()));
        }
        isOriginal = splitInfo.isOriginal;
        hasBase = splitInfo.hasBase;
        deltas = splitInfo.deltas;
        covered = Lists.newArrayListWithCapacity(splitInfo.covered.length);
        for (Boolean bool : splitInfo.covered) {
          covered.add(bool);
        }
      }

      // TODO: Find a way to do this through Guava. Ick!
      private static boolean[] toBooleanArray(List<Boolean> boxedBooleans) {
        boolean[] bools = new boolean[boxedBooleans.size()];
        for (int i = 0; i < boxedBooleans.size(); ++i) {
          bools[i] = boxedBooleans.get(i);
        }
        return bools;
      }

      public OrcInputFormat.SplitInfo toSplitInfo(OrcInputFormat.Context context) throws IOException {
        Path dirPath = new Path(dirName);
        Path filePath = new Path(fileName);
        FileSystem fileSystem = dirPath.getFileSystem(context.conf);
        Reader orcReader = OrcFile.createReader(filePath, OrcFile.readerOptions(context.conf));
        FileStatus fileStatus = fileSystem.getFileStatus(filePath);
        return new OrcInputFormat.SplitInfo(context,
            fileSystem,
            fileStatus,
            new OrcInputFormat.FileInfo(fileStatus.getModificationTime(),
                fileStatus.getLen(),
                orcReader.getStripes(),
                orcReader.getMetadata(),
                orcReader.getTypes(),
                context.footerInSplits ?
                    ((ReaderImpl) orcReader).getFileMetaInfo()
                    : null,
                orcReader.getWriterVersion()),
            isOriginal,
            deltas,
            hasBase,
            dirPath,
            toBooleanArray(covered));
      }

      public long getLength() throws IOException {
        return fileSize;
      }

      public List<String> getLocations() throws IOException {
        return blockHosts;
      }

      @Override
      public void write(DataOutput out) throws IOException {

        Text.writeString(out, fileName);
        Text.writeString(out, dirName);
        out.writeLong(fileSize);

        // Write block-hosts.
        out.writeInt(blockHosts.size());
        for (String blockHost : blockHosts) {
          Text.writeString(out, blockHost);
        }

        out.writeBoolean(isOriginal);
        out.writeBoolean(hasBase);

        // Write deltas.
        out.writeInt(deltas.size());
        for (Long delta : deltas) {
          out.writeLong(delta);
        }

        // Write covered.
        out.writeInt(covered.size());
        for (Boolean bool : covered) {
          out.writeBoolean(bool);
        }
      }

      @Override
      public void readFields(DataInput in) throws IOException {
        fileName = Text.readString(in);
        dirName = Text.readString(in);
        fileSize = in.readLong();

        // Read block-hosts.
        int nBlockHosts = in.readInt();
        blockHosts = Lists.newArrayListWithCapacity(nBlockHosts);
        for (int i = 0; i < nBlockHosts; i++) {
          blockHosts.add(Text.readString(in));
        }

        isOriginal = in.readBoolean();
        hasBase = in.readBoolean();

        // Read deltas.
        int nDeltas = in.readInt();
        deltas = Lists.newArrayListWithCapacity(nDeltas);
        for (int i = 0; i < nDeltas; i++) {
          deltas.add(in.readLong());
        }

        // Read covered.
        int nCovered = in.readInt();
        covered = Lists.newArrayListWithCapacity(nCovered);
        for (int i = 0; i < nCovered; i++) {
          covered.add(in.readBoolean());
        }
      }
    } // class SplitGeneratorSplit.Impl;

    private List<Impl> impls;

    public SplitGeneratorSplit() {
      // Empty default constructor. Used via reflection, when deserializing input-splits.
    }

    public SplitGeneratorSplit(List<OrcInputFormat.SplitInfo> splitInfos) throws IOException {
      impls = Lists.newArrayListWithCapacity(splitInfos.size());
      for(OrcInputFormat.SplitInfo splitInfo : splitInfos) {
        impls.add(new Impl(splitInfo));
      }
    }

    public List<OrcInputFormat.SplitInfo> getSplitInfos(OrcInputFormat.Context context) throws IOException {
      List<OrcInputFormat.SplitInfo> splitInfos = Lists.newArrayListWithCapacity(impls.size());
      for (Impl impl : impls) {
        splitInfos.add(impl.toSplitInfo(context));
      }
      return splitInfos;
    }

    @Override
    public long getLength() throws IOException {
      long length = 0;
      for (Impl impl: impls) {
        length += impl.getLength();
      }
      return length;
    }

    @Override
    public String[] getLocations() throws IOException {
      List<String> locations = Lists.newArrayList();
      for (Impl impl : impls) {
        locations.addAll(impl.getLocations());
      }
      return locations.toArray(new String[locations.size()]);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(impls.size());
      for (Impl impl : impls) {
        impl.write(out);
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      int size = in.readInt();
      impls = Lists.newArrayListWithCapacity(size);
      for (int i = 0; i < size; i++) {
        Impl impl = new Impl();
        impl.readFields(in);
        impls.add(impl);
      }
    }
  } // class SplitGeneratorSplit;

  public static final class SplitGeneratorRecordReader extends org.apache.hadoop.mapreduce.RecordReader<NullWritable, OrcSplit> {

    private List<OrcSplit> orcSplits = Lists.newArrayList();
    private Iterator<OrcSplit> iterator;

    @Override
    public void initialize(org.apache.hadoop.mapreduce.InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      orcSplits = Lists.newArrayList();
      for (OrcInputFormat.SplitInfo splitInfo : ((SplitGeneratorSplit)split).getSplitInfos(new OrcInputFormat.Context(context.getConfiguration()))) {
        orcSplits.addAll(new OrcInputFormat.SplitGenerator(splitInfo, null).call());
      }
      LOG.debug("SplitGeneratorRecordReader::initialize(): Got " + orcSplits.size() + " splits.");
      iterator = orcSplits.iterator();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return iterator.hasNext();
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
      return NullWritable.get();
    }

    @Override
    public OrcSplit getCurrentValue() throws IOException, InterruptedException {
      return iterator.next();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      // Unnecessary. All splits are pre-fetched, during initialization.
      return 0;
    }

    @Override
    public void close() throws IOException {
      // Empty.
    }
  }

  public static final class SplitGeneratorRecordWriter extends org.apache.hadoop.mapreduce.RecordWriter<NullWritable, OrcSplit> {

    Path outputPath;
    DataOutputStream splitList;

    SplitGeneratorRecordWriter(Path outputPath, Configuration conf) throws IOException {
      LOG.debug("Creating SplitGeneratorRecordWriter for outputPath: " + outputPath.toString());
      FileSystem fileSystem = FileSystem.get(conf);
      this.outputPath =  outputPath;
      fileSystem.mkdirs(outputPath);
      this.splitList  = fileSystem.create(new Path(outputPath, SPLIT_LIST_FILE_NAME));
    }

    @Override
    public void write(NullWritable ignore, OrcSplit value) throws IOException, InterruptedException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Writing OrcSplit: " + value.getPath() + ":" + value.getStart() + ":" + value.getLength());
      }
      value.write(splitList);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      LOG.debug("Closing SplitGeneratorRecordWriter for path: " + outputPath);
      IOUtils.cleanup(LOG, splitList);
    }
  }

  public static final class SplitGeneratorInputFormat extends org.apache.hadoop.mapreduce.InputFormat<NullWritable, OrcSplit> {

    @Override
    public List<org.apache.hadoop.mapreduce.InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
      Object splitsObject
          = (deserialize(context.getConfiguration().get(SERIALIZED_SPLIT_INFOS)));
      assert splitsObject == null || splitsObject instanceof ArrayList : "Invalid serialized splits.";

      if (splitsObject == null) {
        return Lists.newArrayList();
      }
      else {
        ArrayList<SplitGeneratorSplit> splitGeneratorSplits = (ArrayList<SplitGeneratorSplit>) splitsObject;
        List<org.apache.hadoop.mapreduce.InputSplit> splits = Lists.newArrayListWithCapacity(splitGeneratorSplits.size());
        splits.addAll(splitGeneratorSplits);
        return splits;
      }
    }

    @Override
    public org.apache.hadoop.mapreduce.RecordReader<NullWritable, OrcSplit>
    createRecordReader(org.apache.hadoop.mapreduce.InputSplit split,
                       TaskAttemptContext context) throws IOException, InterruptedException {
      return new SplitGeneratorRecordReader();
    }
  }

  public static final class SplitGeneratorMapper extends org.apache.hadoop.mapreduce.Mapper<NullWritable, OrcSplit, NullWritable, OrcSplit> {
    @Override
    protected void map(NullWritable key, OrcSplit value, Context context) throws IOException, InterruptedException {
      super.map(key, value, context);
    }
  }

  public static final class SplitGeneratorReducer extends org.apache.hadoop.mapreduce.Reducer<NullWritable, OrcSplit, NullWritable, OrcSplit> {
    @Override
    protected void reduce(NullWritable key, Iterable<OrcSplit> values, Context context) throws IOException, InterruptedException {
      for (OrcSplit split : values) {
        context.write(key, split);
      }
    }
  }

  public static final class SplitGeneratorOutputFormat extends FileOutputFormat<NullWritable, OrcSplit> {

    @Override
    public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
      return new FileOutputCommitter(new Path(context.getConfiguration().get(FileOutputFormat.OUTDIR)), context);
    }

    @Override
    public RecordWriter<NullWritable, OrcSplit> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
      return new SplitGeneratorRecordWriter(((FileOutputCommitter)getOutputCommitter(context)).getWorkPath(),
                                            context.getConfiguration());
    }
  }

  @Override
  public List<OrcSplit> call() throws Exception {
    if (ugi == null) {
      return callInternal();
    }
    try {
      return ugi.doAs(new PrivilegedExceptionAction<List<OrcSplit>>() {
        @Override
        public List<OrcSplit> run() throws Exception {
          return callInternal();
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  private List<OrcSplit> callInternal() throws Exception {
    String scratchDir = HiveConf.getVar(context.conf, HiveConf.ConfVars.SCRATCHDIR)
                        + "/" + UserGroupInformation.getCurrentUser().getUserName();
    Path splitOutputPath = new Path(scratchDir, "split_info_" + String.valueOf(new Random().nextLong()));
    LOG.debug("MapReduceBasedSplitGenerator::call()! OutputPath == " + splitOutputPath);

    try {

      context.conf.set(SERIALIZED_SPLIT_INFOS,
                       serialize(createSplitGeneratorSplits(context.conf, splitInfos)));

      // Artifact of reusing Configurations. Unavoidable, so as not to lose ORC-specific settings.
      unsetOldStyleMRSettings(context.conf);

      Job job = createSplitCalculationJob(context.conf, splitOutputPath);
      job.submit();
      if (!job.waitForCompletion(true)) {
        throw new IOException("Split calculation has failed!");
      } else {
        LOG.debug("Split calculation has succeeded!");
        return readResults(context.conf, splitOutputPath);
      }
    }
    finally {
      FileSystem.get(context.conf).delete(splitOutputPath, true);
    }
  }

  private static Configuration unsetOldStyleMRSettings(Configuration conf) {
    LOG.trace("Purging old-style MR settings. ");
    conf.unset("mapred.mapper.class");
    conf.unset("mapred.reducer.class");
    conf.unset("mapred.input.format.class");
    conf.unset("mapred.partitioner.class");
    conf.unset("mapred.output.format.class");
    conf.unset("mapred.output.committer.class");
    conf.unset("mapreduce.job.output.key.class");
    conf.unset("tmpjars");
    return conf;
  }

  private static Job createSplitCalculationJob(Configuration conf, Path splitOutputPath) throws IOException {
    Job job = Job.getInstance(conf, "MR-based ORC-split-gen");
    job.setJarByClass(MapReduceBasedSplitGenerator.class);
    job.setInputFormatClass(SplitGeneratorInputFormat.class);
    job.setOutputFormatClass(SplitGeneratorOutputFormat.class);
    job.setMapperClass(SplitGeneratorMapper.class);
    job.setNumReduceTasks(1);
    job.setReducerClass(SplitGeneratorReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(OrcSplit.class);
    job.getConfiguration().setBoolean("mapred.mapper.new-api", true);
    FileOutputFormat.setOutputPath(job, splitOutputPath);
    return job;
  }

  private static ArrayList<SplitGeneratorSplit> createSplitGeneratorSplits(
                                                        Configuration conf,
                                                        ArrayList<OrcInputFormat.SplitInfo> splitInfos)
                                                            throws IOException {
    ArrayList<SplitGeneratorSplit> splitGeneratorSplits = Lists.newArrayList();
    int nFilesPerSplit = conf.getInt(HiveConf.ConfVars.HIVE_ORC_SPLIT_STRATEGY_ETL_MR_FILES_PER_SPLIT.name(),
                                     HiveConf.ConfVars.HIVE_ORC_SPLIT_STRATEGY_ETL_MR_FILES_PER_SPLIT.defaultIntVal);
    for (List<OrcInputFormat.SplitInfo> partition : Lists.partition(splitInfos, nFilesPerSplit)) {
      splitGeneratorSplits.add(new SplitGeneratorSplit(partition));
    }
    return splitGeneratorSplits;
  }

  private static List<OrcSplit> readResults(Configuration conf, Path outputPath) throws IOException {
    List<OrcSplit> splits = Lists.newArrayList();
    FileSystem fileSystem = FileSystem.get(conf);
    FSDataInputStream inFile = fileSystem.open(new Path(outputPath, SPLIT_LIST_FILE_NAME));
    LOG.debug("Splits generated in split.list: ");
    while (inFile.available() != 0) {
      OrcSplit orcSplit = new OrcSplit();
      orcSplit.readFields(inFile);
      if (LOG.isDebugEnabled()) {
        LOG.debug("\t" + orcSplit.toString());
      }
      splits.add(orcSplit);
    }
    return splits;
  }

  // Utilities for serializing/deserializing splits.
  static String serialize(Serializable listOfSplitGeneratorSplits) throws IOException {

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
    objectOutputStream.writeObject(listOfSplitGeneratorSplits);
    objectOutputStream.close();

    // Encode the byte-array.
    StringBuilder stringified = new StringBuilder();
    for (byte bite : byteArrayOutputStream.toByteArray()) {
      stringified.append((char) (((bite >> 4) & 0xF) + ('a')));
      stringified.append((char) ((bite & 0xF) + ('a')));
    }

    return stringified.toString();
  }

  static Object deserialize(String stringified) throws IOException {
    try {

      if (org.apache.commons.lang.StringUtils.isBlank(stringified)) {
        return null;
      }

      // Decode the string;
      byte[] bytes = new byte[stringified.length() / 2];
      for (int i = 0; i < stringified.length(); i += 2) {
        char ch = stringified.charAt(i);
        bytes[i / 2] = (byte) ((ch - 'a') << 4);
        ch = stringified.charAt(i + 1);
        bytes[i / 2] += (ch - 'a');
      }

      return new ObjectInputStream(new ByteArrayInputStream(bytes)).readObject();
    }
    catch (Exception exception) {
      throw new IOException("Could not deserialize: " + exception);
    }
  }
}
