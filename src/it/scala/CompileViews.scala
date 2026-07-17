package wabase.app

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.wabase._

class CompileViews extends AnyFlatSpec with Matchers with QuereaseProvider with Loggable {
  it should "compile views" in {
    val previouslyCompiledQueries: Set[String] = Set.empty
    val showFailedViewQuery = true
    val (compiledViews, caches) =
      new AppQuerease with compiling.WabaseViewCompiler{}.compileAllQueries(
        previouslyCompiledQueries,
        showFailedViewQuery,
        logger.info(_: String),
      )
    compiledViews.nonEmpty shouldBe true
  }
}
