import SwiftUI

@main
struct WoladenApp: App {
    @Environment(\.scenePhase) private var scenePhase
    @UIApplicationDelegateAdaptor(AppDelegate.self) private var appDelegate
    @StateObject private var viewModel = AppViewModel()
    @StateObject private var locationService = LocationService()
    @StateObject private var favoritesStore = FavoritesStore()

    var body: some Scene {
        WindowGroup {
            ZStack {
                Color(.systemBackground).ignoresSafeArea()

                RootTabView()
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
            }
            .environmentObject(viewModel)
            .environmentObject(locationService)
            .environmentObject(favoritesStore)
            .task {
                locationService.activate()
                viewModel.load(userLocation: locationService.currentLocation)
            }
            .onChange(of: scenePhase) { _, newValue in
                if newValue == .active {
                    locationService.activate()
                }
            }
            .onChange(of: viewModel.allFeatures.count) { _, _ in
                viewModel.seedFromInitialUserLocation(locationService.currentLocation)
            }
            .onChange(of: locationService.currentLocation) { _, newValue in
                viewModel.seedFromInitialUserLocation(newValue)
            }
        }
    }
}
