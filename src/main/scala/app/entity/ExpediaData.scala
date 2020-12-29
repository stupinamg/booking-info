package app.entity

class ExpediaData (val id: Long,
                   val dateTime: String,
                   val siteName: Int,
                   val posaContinent: Int,
                   val userLocationCountry: Int,
                   val userLocationRegion: Int,
                   val userLocationCity: Int,
                   val origDestinationDistance: AnyVal,
                   val userId: Int,
                   val isMobile: Int,
                   val isPackage: Int,
                   val channel: Int,
                   val srchCi: String,
                   val srchCo: String,
                   val srchAdultsCnt: Int,
                   val srchChildrenCnt: Int,
                   val srchRmCnt: Int,
                   val srchDestinationId: Int,
                   val srchDestinationTypeId: Int) extends Serializable

