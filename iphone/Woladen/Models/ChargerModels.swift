import Foundation
import CoreLocation

private let maxReasonableDisplayPowerKW = 400.0

struct GeoJSONFeatureCollection: Decodable {
    let generatedAt: String?
    let features: [GeoJSONFeature]

    enum CodingKeys: String, CodingKey {
        case generatedAt = "generated_at"
        case features
    }
}

struct GeoJSONFeature: Decodable, Identifiable {
    let id: String
    let geometry: GeoJSONPointGeometry
    let properties: ChargerProperties

    enum CodingKeys: String, CodingKey {
        case geometry
        case properties
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        geometry = try container.decode(GeoJSONPointGeometry.self, forKey: .geometry)
        properties = try container.decode(ChargerProperties.self, forKey: .properties)
        id = properties.stationID
    }

    var coordinate: CLLocationCoordinate2D {
        geometry.coordinate
    }
}

struct GeoJSONPointGeometry: Decodable {
    let type: String
    let coordinates: [Double]

    var coordinate: CLLocationCoordinate2D {
        guard coordinates.count == 2 else {
            return CLLocationCoordinate2D(latitude: 0, longitude: 0)
        }
        return CLLocationCoordinate2D(latitude: coordinates[1], longitude: coordinates[0])
    }
}

struct ChargerProperties: Decodable {
    let stationID: String
    let operatorName: String
    let status: String
    let maxPowerKW: Double
    let chargingPointsCount: Int
    let maxIndividualPowerKW: Double
    let postcode: String
    let city: String
    let address: String
    let occupancySourceUID: String
    let occupancySourceName: String
    let occupancyStatus: String
    let occupancyLastUpdated: String
    let occupancyTotalEVSEs: Int
    let occupancyAvailableEVSEs: Int
    let occupancyOccupiedEVSEs: Int
    let occupancyChargingEVSEs: Int
    let occupancyOutOfOrderEVSEs: Int
    let occupancyUnknownEVSEs: Int
    let amenitiesTotal: Int
    let amenitiesSource: String
    let amenityExamples: [AmenityExample]
    let amenityCounts: [String: Int]

    enum CodingKeys: String, CodingKey {
        case stationID = "station_id"
        case operatorName = "operator"
        case status
        case maxPowerKW = "max_power_kw"
        case chargingPointsCount = "charging_points_count"
        case maxIndividualPowerKW = "max_individual_power_kw"
        case postcode
        case city
        case address
        case occupancySourceUID = "occupancy_source_uid"
        case occupancySourceName = "occupancy_source_name"
        case occupancyStatus = "occupancy_status"
        case occupancyLastUpdated = "occupancy_last_updated"
        case occupancyTotalEVSEs = "occupancy_total_evses"
        case occupancyAvailableEVSEs = "occupancy_available_evses"
        case occupancyOccupiedEVSEs = "occupancy_occupied_evses"
        case occupancyChargingEVSEs = "occupancy_charging_evses"
        case occupancyOutOfOrderEVSEs = "occupancy_out_of_order_evses"
        case occupancyUnknownEVSEs = "occupancy_unknown_evses"
        case amenitiesTotal = "amenities_total"
        case amenitiesSource = "amenities_source"
        case amenityExamples = "amenity_examples"
    }

    init(
        stationID: String,
        operatorName: String,
        status: String,
        maxPowerKW: Double,
        chargingPointsCount: Int,
        maxIndividualPowerKW: Double,
        postcode: String,
        city: String,
        address: String,
        occupancySourceUID: String,
        occupancySourceName: String,
        occupancyStatus: String,
        occupancyLastUpdated: String,
        occupancyTotalEVSEs: Int,
        occupancyAvailableEVSEs: Int,
        occupancyOccupiedEVSEs: Int,
        occupancyChargingEVSEs: Int,
        occupancyOutOfOrderEVSEs: Int,
        occupancyUnknownEVSEs: Int,
        amenitiesTotal: Int,
        amenitiesSource: String,
        amenityExamples: [AmenityExample],
        amenityCounts: [String: Int]
    ) {
        self.stationID = stationID
        self.operatorName = operatorName
        self.status = status
        self.maxPowerKW = maxPowerKW
        self.chargingPointsCount = chargingPointsCount
        self.maxIndividualPowerKW = maxIndividualPowerKW
        self.postcode = postcode
        self.city = city
        self.address = address
        self.occupancySourceUID = occupancySourceUID
        self.occupancySourceName = occupancySourceName
        self.occupancyStatus = occupancyStatus
        self.occupancyLastUpdated = occupancyLastUpdated
        self.occupancyTotalEVSEs = occupancyTotalEVSEs
        self.occupancyAvailableEVSEs = occupancyAvailableEVSEs
        self.occupancyOccupiedEVSEs = occupancyOccupiedEVSEs
        self.occupancyChargingEVSEs = occupancyChargingEVSEs
        self.occupancyOutOfOrderEVSEs = occupancyOutOfOrderEVSEs
        self.occupancyUnknownEVSEs = occupancyUnknownEVSEs
        self.amenitiesTotal = amenitiesTotal
        self.amenitiesSource = amenitiesSource
        self.amenityExamples = amenityExamples
        self.amenityCounts = amenityCounts
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)

        stationID = try container.decode(String.self, forKey: .stationID)
        operatorName = try container.decode(String.self, forKey: .operatorName)
        status = (try? container.decode(String.self, forKey: .status)) ?? ""
        maxPowerKW = container.decodeLossyDouble(forKey: .maxPowerKW) ?? 0
        chargingPointsCount = Int(container.decodeLossyDouble(forKey: .chargingPointsCount) ?? 1)
        maxIndividualPowerKW = container.decodeLossyDouble(forKey: .maxIndividualPowerKW) ?? maxPowerKW
        postcode = (try? container.decode(String.self, forKey: .postcode)) ?? ""
        city = (try? container.decode(String.self, forKey: .city)) ?? ""
        address = (try? container.decode(String.self, forKey: .address)) ?? ""
        occupancySourceUID = (try? container.decode(String.self, forKey: .occupancySourceUID)) ?? ""
        occupancySourceName = (try? container.decode(String.self, forKey: .occupancySourceName)) ?? ""
        occupancyStatus = (try? container.decode(String.self, forKey: .occupancyStatus)) ?? ""
        occupancyLastUpdated = (try? container.decode(String.self, forKey: .occupancyLastUpdated)) ?? ""
        occupancyTotalEVSEs = Int(container.decodeLossyDouble(forKey: .occupancyTotalEVSEs) ?? 0)
        occupancyAvailableEVSEs = Int(container.decodeLossyDouble(forKey: .occupancyAvailableEVSEs) ?? 0)
        occupancyOccupiedEVSEs = Int(container.decodeLossyDouble(forKey: .occupancyOccupiedEVSEs) ?? 0)
        occupancyChargingEVSEs = Int(container.decodeLossyDouble(forKey: .occupancyChargingEVSEs) ?? 0)
        occupancyOutOfOrderEVSEs = Int(container.decodeLossyDouble(forKey: .occupancyOutOfOrderEVSEs) ?? 0)
        occupancyUnknownEVSEs = Int(container.decodeLossyDouble(forKey: .occupancyUnknownEVSEs) ?? 0)
        amenitiesTotal = Int(container.decodeLossyDouble(forKey: .amenitiesTotal) ?? 0)
        amenitiesSource = (try? container.decode(String.self, forKey: .amenitiesSource)) ?? ""
        amenityExamples = (try? container.decode([AmenityExample].self, forKey: .amenityExamples)) ?? []

        let raw = try decoder.container(keyedBy: AnyCodingKey.self)
        var collected: [String: Int] = [:]
        for key in raw.allKeys where key.stringValue.hasPrefix("amenity_") {
            let value: Int
            if let intValue = try? raw.decode(Int.self, forKey: key) {
                value = intValue
            } else if let doubleValue = try? raw.decode(Double.self, forKey: key) {
                value = Int(doubleValue)
            } else if let stringValue = try? raw.decode(String.self, forKey: key) {
                value = Int(stringValue) ?? 0
            } else {
                value = 0
            }
            collected[key.stringValue] = value
        }
        amenityCounts = collected
    }
}

struct AmenityExample: Decodable, Identifiable {
    let id = UUID()
    let category: String
    let name: String?
    let openingHours: String?
    let distanceM: Double?
    let lat: Double?
    let lon: Double?

    enum CodingKeys: String, CodingKey {
        case category
        case name
        case openingHours = "opening_hours"
        case distanceM = "distance_m"
        case lat
        case lon
    }

    var coordinate: CLLocationCoordinate2D? {
        guard let lat, let lon else { return nil }
        return CLLocationCoordinate2D(latitude: lat, longitude: lon)
    }
}

extension ChargerProperties {
    var displayedMaxPowerKW: Double {
        let maxIndividual = sanitizedDisplayPower(maxIndividualPowerKW)
        if maxIndividual > 0 {
            return maxIndividual
        }
        return sanitizedDisplayPower(maxPowerKW)
    }

    func topAmenities(limit: Int = 3) -> [AmenityCount] {
        amenityCounts
            .filter { $0.value > 0 }
            .map { AmenityCount(key: $0.key, count: $0.value) }
            .sorted { lhs, rhs in
                if lhs.count == rhs.count { return lhs.key < rhs.key }
                return lhs.count > rhs.count
            }
            .prefix(limit)
            .map { $0 }
    }

    var occupancySummaryLabel: String? {
        guard occupancyTotalEVSEs > 0 else { return nil }
        if occupancyAvailableEVSEs > 0 {
            return "\(occupancyAvailableEVSEs)/\(occupancyTotalEVSEs) frei"
        }
        if occupancyOccupiedEVSEs > 0 {
            return "\(occupancyOccupiedEVSEs)/\(occupancyTotalEVSEs) belegt"
        }
        if occupancyOutOfOrderEVSEs >= occupancyTotalEVSEs {
            return "Außer Betrieb"
        }
        return "Belegung unbekannt"
    }

    var occupancySourceLabel: String? {
        guard occupancyTotalEVSEs > 0 else { return nil }
        if occupancySourceName.isEmpty {
            return "Live via MobiData BW"
        }
        return "Live via MobiData BW (\(occupancySourceName))"
    }
}

private func sanitizedDisplayPower(_ value: Double) -> Double {
    guard value.isFinite, value > 0 else { return 0 }
    return min(value, maxReasonableDisplayPowerKW)
}

struct AmenityCount: Identifiable {
    var id: String { key }
    let key: String
    let count: Int
}

struct AnyCodingKey: CodingKey {
    let stringValue: String
    let intValue: Int?

    init?(stringValue: String) {
        self.stringValue = stringValue
        self.intValue = nil
    }

    init?(intValue: Int) {
        self.stringValue = "\(intValue)"
        self.intValue = intValue
    }
}

extension KeyedDecodingContainer {
    fileprivate func decodeLossyDouble(forKey key: Key) -> Double? {
        if let value = try? decode(Double.self, forKey: key) {
            return value
        }
        if let value = try? decode(Int.self, forKey: key) {
            return Double(value)
        }
        if let string = try? decode(String.self, forKey: key) {
            let normalized = string.replacingOccurrences(of: ",", with: ".")
            return Double(normalized)
        }
        return nil
    }
}
