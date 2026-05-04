const CACHE_NAME = "muuc-finance-v1";
const APP_SHELL = [
  "/",
  "/login",
  "/dashboard",
  "/rules",
  "/files",
  "/static/app.css",
  "/static/manifest.webmanifest",
  "/static/icons/icon-192.png",
  "/static/icons/icon-512.png",
  "/static/icons/apple-touch-icon.png",
  "/static/icons/favicon-32.png",
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(APP_SHELL)).then(() => self.skipWaiting())
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", (event) => {
  if (event.request.method !== "GET") {
    return;
  }

  const requestUrl = new URL(event.request.url);
  const isStaticAsset = requestUrl.pathname.startsWith("/static/");
  const isHtmlPage = event.request.mode === "navigate";

  if (isStaticAsset) {
    event.respondWith(
      caches.match(event.request).then((cached) => cached || fetch(event.request))
    );
    return;
  }

  if (isHtmlPage) {
    event.respondWith(
      fetch(event.request)
        .then((response) => {
          const copy = response.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put(event.request, copy));
          return response;
        })
        .catch(() => caches.match(event.request).then((cached) => cached || caches.match("/login")))
    );
  }
});
