import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import util.Protocol.{Commit, CommitGeo, CommitSummary, File}
import util.{CommitGeoParser, CommitParser}

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.functions.windowing.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.util.Collector

import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor

import org.apache.flink.cep.scala.pattern.{Pattern => ScalaPattern}
import org.apache.flink.cep.scala.{CEP => ScalaCEP}
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.PatternSelectFunction
import scala.collection.JavaConverters._

/** Do NOT rename this class, otherwise autograding will fail. **/
object FlinkAssignment {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  def main(args: Array[String]): Unit = {

    /**
      * Setups the streaming environment including loading and parsing of the datasets.
      *
      * DO NOT TOUCH!
      */
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // Read and parses commit stream.
    val commitStream =
      env
        .readTextFile("data/flink_commits.json")
        .map(new CommitParser)

    // Read and parses commit geo stream.
    val commitGeoStream =
      env
        .readTextFile("data/flink_commits_geo.json")
        .map(new CommitGeoParser)

    /** Use the space below to print and test your questions. */
    dummy_question(commitStream).print()

    /** Start the streaming environment. **/
    env.execute()
  }

  /** Dummy question which maps each commits to its SHA. */
  def dummy_question(input: DataStream[Commit]): DataStream[String] = {
    input.map(_.sha)
  }

  /**
    * Write a Flink application which outputs the sha of commits with at least 20 additions.
    * Output format: sha
    */
  def question_one(input: DataStream[Commit]): DataStream[String] = input
    .flatMap { commit =>
      if (commit.files.exists(_.additions > 20)) {
        Some(commit.sha)
      }
      else {
        None
      }
    }

  /**
    * Write a Flink application which outputs the names of the files with more than 30 deletions.
    * Output format:  fileName
    */
  def question_two(input: DataStream[Commit]): DataStream[String] = input
    .flatMap(_.files)
    .filter(_.deletions > 30)
    .flatMap(_.filename)

  /**
    * Count the occurrences of Java and Scala files. I.e. files ending with either .scala or .java.
    * Output format: (fileExtension, #occurrences)
    */
  def question_three(input: DataStream[Commit]): DataStream[(String, Int)] = {
    input
      .flatMap(commit => commit.files.flatMap(_.filename))
      .filter(name => name.endsWith(".java") || name.endsWith(".scala"))
      .map { name =>
        val ext = if (name.endsWith(".java")) "java" else "scala"
        (ext, 1)
      }
      .keyBy(_._1)
      .sum(1)
  }

  /**
    * Count the total amount of changes for each file status (e.g. modified, removed or added) for the following extensions: .js and .py.
    * Output format: (extension, status, count)
    */
  def question_four(
      input: DataStream[Commit]): DataStream[(String, String, Int)] = {

    input
      .flatMap(_.files)
      .filter(f => f.filename.exists(name => name.endsWith(".js") || name.endsWith(".py")))
      .filter(f => f.status.isDefined)
      .map { f =>
        val name = f.filename.get
        val status = f.status.get
        val ext = if (name.endsWith(".js")) "js" else "py"
        (ext, status, f.changes)
      }
      .keyBy(_._1, _._2)
      .sum(2)
  }

  /**
    * For every day output the amount of commits. Include the timestamp in the following format dd-MM-yyyy; e.g. (26-06-2019, 4) meaning on the 26th of June 2019 there were 4 commits.
    * Make use of a non-keyed window.
    * Output format: (date, count)
    */
  def question_five(input: DataStream[Commit]): DataStream[(String, Int)] = {
    val fmt = new SimpleDateFormat("dd-MM-yyyy")

    input
      .timeWindowAll(Time.days(1))
      .process(new ProcessAllWindowFunction[Commit, (String, Int), TimeWindow] {
        override def process(context: Context, elements: Iterable[Commit], out: Collector[(String, Int)]): Unit = {
          val windowStart = new Date(context.window.getStart)
          val dateStr = fmt.format(windowStart)
          val count = elements.size
          out.collect((dateStr, count))
        }
      })
  }


  /**
    * Consider two types of commits; small commits and large commits whereas small: 0 <= x <= 20 and large: x > 20 where x = total amount of changes.
    * Compute every 12 hours the amount of small and large commits in the last 48 hours.
    * Output format: (type, count)
    */
  def question_six(input: DataStream[Commit]): DataStream[(String, Int)] = {
    input
      .map { commit =>
        // Calculate total changes for the commit
        val totalChanges = commit.stats.map(_.total).getOrElse(commit.files.map(_.changes).sum)
        val commitType = if (totalChanges <= 20) "small" else "large"
        (commitType, 1)
      }
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.hours(48), Time.hours(12)))
      .sum(1)
  }

  /**
    * For each repository compute a daily commit summary and output the summaries with more than 20 commits and at most 2 unique committers. The CommitSummary case class is already defined.
    *
    * The fields of this case class:
    *
    * repo: name of the repo.
    * date: use the start of the window in format "dd-MM-yyyy".
    * amountOfCommits: the number of commits on that day for that repository.
    * amountOfCommitters: the amount of unique committers contributing to the repository.
    * totalChanges: the sum of total changes in all commits.
    * topCommitter: the top committer of that day i.e. with the most commits. Note: if there are multiple top committers; create a comma separated string sorted alphabetically e.g. `georgios,jeroen,wouter`
    *
    * Hint: Write your own ProcessWindowFunction.
    * Output format: CommitSummary
    */
  def question_seven(
      commitStream: DataStream[Commit]): DataStream[CommitSummary] = {
    val fmt = new SimpleDateFormat("dd-MM-yyyy")

    def extractRepoFromUrl(url: String): String = {
      // common GitHub commit URL contains /repos/{owner}/{repo}/commits/{sha}
      // attempt to find "repos" and take next two segments
      try {
        val parts = url.split("/")
        val idx = parts.indexOf("repos")
        if (idx >= 0 && idx + 2 < parts.length) {
          s"${parts(idx + 1)}/${parts(idx + 2)}"
        } else {
          // fallback: try owner/repo from URL by heuristic
          val repoIdx = parts.indexWhere(_.contains("github.com"))
          if (repoIdx >= 0 && repoIdx + 2 < parts.length) s"${parts(repoIdx + 1)}/${parts(repoIdx + 2)}"
          else url
        }
      } catch {
        case _: Throwable => url
      }
    }

    // key by repository
    val keyedByRepo = commitStream
      .map { c =>
        val repo = extractRepoFromUrl(c.url)
        (repo, c)
      }
      .keyBy(_._1)

    keyedByRepo
      .timeWindow(Time.days(1))
      .process(new ProcessWindowFunction[(String, Commit), CommitSummary, String, TimeWindow] {
        override def process(key: String,
                             ctx: Context,
                             elements: Iterable[(String, Commit)],
                             out: Collector[CommitSummary]): Unit = {

          val commits: Seq[Commit] = elements.toSeq.map(_._2)
          val amountOfCommits = commits.size

          // committers by name (use commit.commit.author.name where available)
          val committerNames = commits.flatMap { c =>
            Option(c.commit.committer).map(_.name) match {
              case Some(name) => Seq(name)
              case None => Option(c.commit.author).map(_.name).toSeq // conservative fallback
            }
          }

          val uniqueCommittersSet = committerNames.toSet
          val amountOfCommitters = uniqueCommittersSet.size

          // total changes: use stats.total if available per commit (prefer that) else sum file.changes
          val totalChanges = commits.map { c => c.stats.map(_.total).getOrElse(c.files.map(_.changes).sum) }.sum

          // top committer(s) by number of commits
          val countsByCommitter = committerNames.groupBy(identity).view.mapValues(_.size).toMap
          val maxCount = if (countsByCommitter.nonEmpty) countsByCommitter.values.max else 0
          val topCommitters = countsByCommitter.filter(_._2 == maxCount).keys.toSeq.sorted
          val topCommitterStr = topCommitters.mkString(",")

          // date = start of the window in dd-MM-yyyy
          val windowStart = new Date(ctx.window.getStart)
          val dateStr = fmt.format(windowStart)

          val summary = CommitSummary(
            repo = key,
            date = dateStr,
            amountOfCommits = amountOfCommits,
            amountOfCommitters = amountOfCommitters,
            totalChanges = totalChanges,
            mostPopularCommitter = topCommitterStr
          )

          // only output where more than 20 commits and at most 2 unique committers
          if (summary.amountOfCommits > 20 && summary.amountOfCommitters <= 2) {
            out.collect(summary)
          }
        }
      })
  }

  /**
    * For this exercise there is another dataset containing CommitGeo events. A CommitGeo event stores the sha of a commit, a date and the continent it was produced in.
    * You can assume that for every commit there is a CommitGeo event arriving within a timeframe of 1 hour before and 30 minutes after the commit.
    * Get the weekly amount of changes for the java files (.java extension) per continent. If no java files are changed in a week, no output should be shown that week.
    *
    * Hint: Find the correct join to use!
    * Output format: (continent, amount)
    */
  def question_eight(
      commitStream: DataStream[Commit],
      geoStream: DataStream[CommitGeo]): DataStream[(String, Int)] = {
    
    // Filter commits to only those with Java files and calculate total changes
    val javaCommits = commitStream
      .filter(commit => commit.files.exists(file => file.filename.exists(_.endsWith(".java"))))
      .map(commit => (commit.sha, commit.files.filter(file => file.filename.exists(_.endsWith(".java"))).map(_.changes).sum))
      .keyBy(_._1)

    // Key geo stream by sha
    val keyedGeoStream = geoStream.keyBy(_.sha)

    // Join streams within the time window (1 hour before to 30 minutes after)
    javaCommits
      .intervalJoin(keyedGeoStream)
      .between(Time.minutes(-30), Time.hours(1))
      .process(new ProcessJoinFunction[(String, Int), CommitGeo, (String, Int)] {
        override def processElement(commit: (String, Int), geo: CommitGeo, context: ProcessJoinFunction[(String, Int), CommitGeo, (String, Int)]#Context, out: Collector[(String, Int)]): Unit = {
          out.collect((geo.continent, commit._2))
        }
      })
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.days(7)))
      .sum(1)
  }

  /**
    * Find all files that were added and removed within one day. Output as (repository, filename).
    *
    * Hint: Use the Complex Event Processing library (CEP).
    * Output format: (repository, filename)
    */
  def question_nine(
      inputStream: DataStream[Commit]): DataStream[(String, String)] = {
    
    // Extract repository from URL
    def extractRepoFromUrl(url: String): String = {
      try {
        val parts = url.split("/")
        val idx = parts.indexOf("repos")
        if (idx >= 0 && idx + 2 < parts.length) {
          s"${parts(idx + 1)}/${parts(idx + 2)}"
        } else {
          url
        }
      } catch {
        case _: Throwable => url
      }
    }

    // Create stream of (repo, filename, status, timestamp) for added and removed files
    val fileEvents = inputStream
      .flatMap { commit =>
        val repo = extractRepoFromUrl(commit.url)
        commit.files.flatMap { file =>
          for {
            filename <- file.filename
            status <- file.status
            if status == "added" || status == "removed"
          } yield (repo, filename, status, commit.commit.committer.date.getTime)
        }
      }
      .keyBy(event => (event._1, event._2)) // Key by (repo, filename)

    // Define CEP pattern: added followed by removed within 1 day
    val pattern = ScalaPattern.begin[(String, String, String, Long)]("added")
      .where(_._3 == "added")
      .followedBy("removed")
      .where(_._3 == "removed")
      .within(Time.days(1))

    // Apply pattern to the stream
    val patternStream = ScalaCEP.pattern(fileEvents, pattern)

    // Select matching patterns
    patternStream.select(new PatternSelectFunction[(String, String, String, Long), (String, String)] {
      override def select(pattern: java.util.Map[String, java.util.List[(String, String, String, Long)]]): (String, String) = {
        val addedEvent = pattern.get("added").asScala.head
        val removedEvent = pattern.get("removed").asScala.head
        
        // Return (repository, filename)
        (addedEvent._1, addedEvent._2)
      }
    })
  }


}
