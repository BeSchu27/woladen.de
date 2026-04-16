import Foundation

enum LiveAPIError: LocalizedError {
    case invalidBaseURL
    case invalidResponse
    case unexpectedStatusCode(Int)

    var errorDescription: String? {
        switch self {
        case .invalidBaseURL:
            return "Ungültige Live-API-URL"
        case .invalidResponse:
            return "Unerwartete Antwort der Live-API"
        case .unexpectedStatusCode(let statusCode):
            return "Live-API antwortete mit HTTP \(statusCode)"
        }
    }
}

final class LiveAPIClient {
    private let baseURL: URL?
    private let session: URLSession
    private let decoder = JSONDecoder()

    init(baseURL: URL? = LiveAPIClient.resolveBaseURL(), session: URLSession = .shared) {
        self.baseURL = baseURL
        self.session = session
    }

    var isEnabled: Bool {
        baseURL != nil
    }

    func lookupStations(stationIDs: [String]) async throws -> LiveStationLookupResponse {
        guard let url = endpointURL(path: "/v1/stations/lookup") else {
            throw LiveAPIError.invalidBaseURL
        }

        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.timeoutInterval = 3.5
        request.setValue("application/json", forHTTPHeaderField: "Accept")
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try JSONSerialization.data(withJSONObject: [
            "station_ids": stationIDs
        ])

        return try await send(request)
    }

    func stationDetail(stationID: String) async throws -> LiveStationDetail {
        guard let encoded = stationID.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed),
              let url = endpointURL(path: "/v1/stations/\(encoded)") else {
            throw LiveAPIError.invalidBaseURL
        }

        var request = URLRequest(url: url)
        request.httpMethod = "GET"
        request.timeoutInterval = 4.0
        request.setValue("application/json", forHTTPHeaderField: "Accept")
        return try await send(request)
    }

    private func endpointURL(path: String) -> URL? {
        baseURL?.appending(path: path)
    }

    private func send<Response: Decodable>(_ request: URLRequest) async throws -> Response {
        let (data, response) = try await session.data(for: request)
        guard let httpResponse = response as? HTTPURLResponse else {
            throw LiveAPIError.invalidResponse
        }
        guard (200..<300).contains(httpResponse.statusCode) else {
            throw LiveAPIError.unexpectedStatusCode(httpResponse.statusCode)
        }
        return try decoder.decode(Response.self, from: data)
    }

    private static func resolveBaseURL() -> URL? {
        if let configured = Bundle.main.object(forInfoDictionaryKey: "WoladenLiveAPIBaseURL") as? String,
           let normalized = normalizedURL(from: configured) {
            return normalized
        }

        if let configured = ProcessInfo.processInfo.environment["WOLADEN_LIVE_API_BASE_URL"],
           let normalized = normalizedURL(from: configured) {
            return normalized
        }

        return URL(string: "https://live.woladen.de")
    }

    private static func normalizedURL(from value: String) -> URL? {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return nil }
        guard let url = URL(string: trimmed) else { return nil }
        let absolute = url.absoluteString.replacingOccurrences(of: "/+$", with: "", options: .regularExpression)
        return URL(string: absolute)
    }
}
