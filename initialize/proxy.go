package initialize

import (
	"aurora/internal/proxys"
	"bufio"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

func checkProxy() *proxys.IProxy {
	baseProxies := readStaticProxies()
	proxyListURL := os.Getenv("PROXY_LIST_URL")
	proxies := mergeProxies(baseProxies, readProxyListURL(proxyListURL))
	proxyIP := proxys.NewIProxyIP(proxies)

	if proxyListURL != "" {
		go refreshProxyList(&proxyIP, baseProxies, proxyListURL, proxyListRefreshInterval())
	}

	return &proxyIP
}

func readStaticProxies() []string {
	var proxies []string
	proxyUrl := os.Getenv("PROXY_URL")
	if proxyUrl != "" {
		proxies = appendProxy(proxies, proxyUrl)
	}

	if _, err := os.Stat("proxies.txt"); err == nil {
		file, _ := os.Open("proxies.txt")
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			proxies = appendProxy(proxies, scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			slog.Warn("proxies.txt read failed", "err", err)
		}
	}

	if len(proxies) == 0 {
		proxy := os.Getenv("http_proxy")
		if proxy != "" {
			proxies = appendProxy(proxies, proxy)
		}
	}
	return proxies
}

func readProxyListURL(listURL string) []string {
	if strings.TrimSpace(listURL) == "" {
		return nil
	}
	client := &http.Client{Timeout: 15 * time.Second}
	response, err := client.Get(listURL)
	if err != nil {
		slog.Warn("proxy list url request failed", "url", listURL, "err", err)
		return nil
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		slog.Warn("proxy list url returned non-200", "url", listURL, "status", response.StatusCode)
		return nil
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		slog.Warn("proxy list url read failed", "url", listURL, "err", err)
		return nil
	}
	var proxies []string
	scanner := bufio.NewScanner(strings.NewReader(string(body)))
	for scanner.Scan() {
		proxies = appendProxy(proxies, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		slog.Warn("proxy list url scan failed", "url", listURL, "err", err)
	}
	return proxies
}

func refreshProxyList(proxyPool *proxys.IProxy, baseProxies []string, listURL string, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		remoteProxies := readProxyListURL(listURL)
		if len(remoteProxies) == 0 && len(baseProxies) == 0 {
			slog.Warn("proxy list refresh returned no proxies; keeping previous proxy pool", "url", listURL)
			continue
		}
		proxyPool.SetIPS(mergeProxies(baseProxies, remoteProxies))
	}
}

func proxyListRefreshInterval() time.Duration {
	value := strings.TrimSpace(os.Getenv("PROXY_LIST_REFRESH_INTERVAL"))
	if value == "" {
		return time.Hour
	}
	interval, err := time.ParseDuration(value)
	if err != nil || interval <= 0 {
		slog.Warn("PROXY_LIST_REFRESH_INTERVAL is invalid, using 1h", "value", value, "err", err)
		return time.Hour
	}
	return interval
}

func mergeProxies(staticProxies []string, remoteProxies []string) []string {
	proxies := make([]string, 0, len(staticProxies)+len(remoteProxies))
	proxies = append(proxies, staticProxies...)
	proxies = append(proxies, remoteProxies...)
	return proxies
}

func appendProxy(proxies []string, proxy string) []string {
	proxy = strings.TrimSpace(proxy)
	if proxy == "" || strings.HasPrefix(proxy, "#") {
		return proxies
	}
	parsedURL, err := url.Parse(proxy)
	if err != nil {
		slog.Warn("proxy url is invalid", "url", proxy, "err", err)
		return proxies
	}
	if parsedURL.Scheme == "" || parsedURL.Host == "" || parsedURL.Port() == "" {
		slog.Warn("proxy url is incomplete", "url", proxy)
		return proxies
	}
	return append(proxies, proxy)
}
