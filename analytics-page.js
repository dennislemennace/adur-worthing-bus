/* Page counting for the static pages (about, privacy, terms), on the same
   terms as loadAnalytics in app.js: nothing is fetched without a site code,
   for a browser that asks not to be tracked, for a reader who has switched
   counting off on the privacy page, or on a local preview. The code must equal
   CONFIG.GOATCOUNTER_CODE in app.js; tests/test_privacy_notice.py checks. */
(function () {
  var code = "dennislemennace";
  if (!/^[a-z0-9-]+$/.test(code)) return;
  try { if (localStorage.getItem("analytics-opt-out") === "1") return; } catch (e) { /* storage blocked */ }
  if (navigator.globalPrivacyControl === true) return;
  var dnt = navigator.doNotTrack || window.doNotTrack;
  if (dnt === "1" || dnt === "yes") return;
  var host = location.hostname || "";
  if (!host || host === "localhost" || /\.local$/.test(host)
      || /^(127|10)\./.test(host) || /^192\.168\./.test(host)
      || /^172\.(1[6-9]|2\d|3[01])\./.test(host)) return;
  var s = document.createElement("script");
  s.async = true;
  s.src = "https://gc.zgo.at/count.js";
  s.setAttribute("data-goatcounter", "https://" + code + ".goatcounter.com/count");
  document.head.appendChild(s);
})();
