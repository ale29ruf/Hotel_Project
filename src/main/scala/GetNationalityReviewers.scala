
object GetNationalityReviewers {

  def get(): List[String] = {
    WebService.dataFrame.select("Reviewer_Nationality").rdd
      .map(_.getString(0)).distinct().collect().toList
  }

}
