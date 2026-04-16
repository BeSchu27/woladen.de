import XCTest
@testable import Woladen

final class LiveFeatureFormattingTests: XCTestCase {
    func testLiveSummaryOverridesBundledOccupancyAndPrice() {
        let feature = sampleFeature(
            properties: sampleProperties(
                occupancyTotalEVSEs: 2,
                occupancyAvailableEVSEs: 2,
                priceDisplay: "ab 0,59 €/kWh",
                detailSourceUID: "mobilithek_enbwmobility_static",
                detailSourceName: "EnBWmobility+"
            ),
            liveSummary: LiveStationSummary(
                stationID: "station-1",
                availabilityStatus: .occupied,
                availableEVSEs: 1,
                occupiedEVSEs: 2,
                outOfOrderEVSEs: 0,
                unknownEVSEs: 0,
                totalEVSEs: 3,
                priceDisplay: "ab 0,69 €/kWh",
                priceCurrency: "EUR",
                priceEnergyEURKwhMin: "0.69",
                priceEnergyEURKwhMax: "0.69",
                sourceObservedAt: "2026-04-16T14:10:02Z",
                fetchedAt: "2026-04-16T14:12:16Z",
                ingestedAt: "2026-04-16T14:12:16Z"
            )
        )

        XCTAssertEqual(feature.displayPrice, "ab 0,69 €/kWh")
        XCTAssertEqual(feature.occupancySummaryLabel, "1 frei, 2 belegt")
        XCTAssertEqual(feature.availabilityStatus, .occupied)
        XCTAssertEqual(feature.liveEVSERows.count, 1)
        XCTAssertTrue(feature.occupancySourceLabel?.contains("EnBWmobility+") == true)
    }

    func testBundledValuesRemainWhenNoLiveOverlayExists() {
        let feature = sampleFeature(
            properties: sampleProperties(
                occupancyTotalEVSEs: 4,
                occupancyAvailableEVSEs: 3,
                occupancyOccupiedEVSEs: 1,
                priceDisplay: "ab 0,59 €/kWh"
            )
        )

        XCTAssertEqual(feature.displayPrice, "ab 0,59 €/kWh")
        XCTAssertEqual(feature.occupancySummaryLabel, "3 frei, 1 belegt")
        XCTAssertEqual(feature.availabilityStatus, .free)
        XCTAssertTrue(feature.liveEVSERows.isEmpty)
    }

    private func sampleFeature(
        properties: ChargerProperties,
        liveSummary: LiveStationSummary? = nil
    ) -> GeoJSONFeature {
        GeoJSONFeature(
            id: properties.stationID,
            geometry: GeoJSONPointGeometry(type: "Point", coordinates: [13.4, 52.5]),
            properties: properties,
            liveSummary: liveSummary
        )
    }

    private func sampleProperties(
        occupancyTotalEVSEs: Int = 0,
        occupancyAvailableEVSEs: Int = 0,
        occupancyOccupiedEVSEs: Int = 0,
        priceDisplay: String = "",
        detailSourceUID: String = "",
        detailSourceName: String = ""
    ) -> ChargerProperties {
        ChargerProperties(
            stationID: "station-1",
            operatorName: "IONITY",
            status: "In Betrieb",
            maxPowerKW: 150,
            chargingPointsCount: 4,
            maxIndividualPowerKW: 150,
            postcode: "10115",
            city: "Berlin",
            address: "Teststraße 1",
            occupancySourceUID: "",
            occupancySourceName: "",
            occupancyStatus: "",
            occupancyLastUpdated: "",
            occupancyTotalEVSEs: occupancyTotalEVSEs,
            occupancyAvailableEVSEs: occupancyAvailableEVSEs,
            occupancyOccupiedEVSEs: occupancyOccupiedEVSEs,
            occupancyChargingEVSEs: 0,
            occupancyOutOfOrderEVSEs: 0,
            occupancyUnknownEVSEs: 0,
            detailSourceUID: detailSourceUID,
            detailSourceName: detailSourceName,
            detailLastUpdated: "",
            datexSiteID: "",
            datexStationIDs: "",
            datexChargePointIDs: "",
            priceDisplay: priceDisplay,
            priceEnergyEURKwhMin: "",
            priceEnergyEURKwhMax: "",
            priceCurrency: "EUR",
            priceQuality: "",
            openingHoursDisplay: "24/7",
            openingHoursIs24_7: true,
            helpdeskPhone: "",
            paymentMethodsDisplay: "",
            authMethodsDisplay: "",
            connectorTypesDisplay: "",
            currentTypesDisplay: "",
            connectorCount: 0,
            greenEnergy: nil,
            serviceTypesDisplay: "",
            detailsJSON: "",
            amenitiesTotal: 0,
            amenitiesSource: "osm-pbf",
            amenityExamples: [],
            amenityCounts: [:]
        )
    }
}
