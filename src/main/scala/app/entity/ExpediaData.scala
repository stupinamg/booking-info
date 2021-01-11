package app.entity

case class ExpediaData(id: Long,
                       dateTime: String,
                       siteName: Int,
                       posaContinent: Int,
                       userLocationCountry: Int,
                       userLocationRegion: Int,
                       userLocationCity: Int,
                       origDestinationDistance: AnyVal,
                       userId: Int,
                       isMobile: Int,
                       isPackage: Int,
                       channel: Int,
                       srchCi: String,
                       srchCo: String,
                       srchAdultsCnt: Int,
                       srchChildrenCnt: Int,
                       srchRmCnt: Int,
                       srchDestinationId: Int,
                       srchDestinationTypeId: Int,
                       hotel_id: Double) extends Serializable

