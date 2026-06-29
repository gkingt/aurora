package initialize

import (
	"aurora/httpclient"
	"aurora/httpclient/bogdanfinn"
	"aurora/internal/proxys"
	"bufio"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func checkProxy() *proxys.IProxy {
	baseProxies := readStaticProxies()
	proxyListURL := os.Getenv("PROXY_LIST_URL")
	proxyIP := proxys.NewIProxyIP(baseProxies)

	if proxyListURL != "" {
		go refreshProxyList(&proxyIP, baseProxies, proxyListURL, proxyListRefreshInterval(), true)
	} else if len(baseProxies) > 0 {
		go func() {
			available := filterAvailableProxies(baseProxies, nil)
			if len(available) > 0 {
				proxyIP.SetIPS(available)
			}
		}()
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

func refreshProxyList(proxyPool *proxys.IProxy, baseProxies []string, listURL string, interval time.Duration, runImmediately bool) {
	refresh := func() {
		if len(baseProxies) > 0 {
			proxyPool.SetIPS(baseProxies)
		}
		remoteProxies := filterAvailableProxies(readProxyListURL(listURL), func(proxy string) {
			added := proxyPool.AddIP(proxy)
			slog.Info("proxy added to active pool", "proxy", proxy, "added", added, "available_proxies", proxyPool.GetIPS())
		})
		if len(remoteProxies) == 0 && len(baseProxies) == 0 {
			slog.Warn("proxy list refresh returned no available proxies; keeping previous proxy pool", "url", listURL)
			return
		}
		proxyPool.SetIPS(mergeProxies(baseProxies, remoteProxies))
		slog.Info("proxy pool refreshed", "base_proxies", len(baseProxies), "remote_available", len(remoteProxies), "available_proxies", proxyPool.GetIPS())
	}
	if runImmediately {
		refresh()
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		refresh()
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

func proxyCheckTimeout() time.Duration {
	value := strings.TrimSpace(os.Getenv("PROXY_CHECK_TIMEOUT"))
	if value == "" {
		return 20 * time.Second
	}
	timeout, err := time.ParseDuration(value)
	if err != nil || timeout <= 0 {
		slog.Warn("PROXY_CHECK_TIMEOUT is invalid, using 20s", "value", value, "err", err)
		return 20 * time.Second
	}
	return timeout
}

func proxyCheckConcurrency() int {
	value := strings.TrimSpace(os.Getenv("PROXY_CHECK_CONCURRENCY"))
	if value == "" {
		return 20
	}
	concurrency, err := strconv.Atoi(value)
	if err != nil || concurrency <= 0 {
		slog.Warn("PROXY_CHECK_CONCURRENCY is invalid, using 20", "value", value, "err", err)
		return 20
	}
	return concurrency
}

func filterAvailableProxies(proxies []string, onAvailable func(string)) []string {
	if len(proxies) == 0 {
		return nil
	}
	timeout := proxyCheckTimeout()
	workerLimit := proxyCheckConcurrency()
	if workerLimit > len(proxies) {
		workerLimit = len(proxies)
	}
	sem := make(chan struct{}, workerLimit)
	available := make([]string, 0, len(proxies))
	var lock sync.Mutex
	var wg sync.WaitGroup
	checked := 0
	for _, proxy := range proxies {
		proxy := proxy
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			ok := checkProxyAvailable(proxy, timeout)
			lock.Lock()
			checked++
			if ok {
				available = append(available, proxy)
			}
			currentChecked := checked
			currentAvailable := len(available)
			lock.Unlock()

			if ok && onAvailable != nil {
				onAvailable(proxy)
			}
			slog.Info("proxy preflight progress", "proxy", proxy, "available", ok, "checked", currentChecked, "total", len(proxies), "available_count", currentAvailable)
		}()
	}
	wg.Wait()
	slog.Info("proxy preflight completed", "total", len(proxies), "available", len(available), "timeout", timeout.String())
	return available
}

func checkProxyAvailable(proxy string, timeout time.Duration) bool {
	client := bogdanfinn.NewStdClientWithTimeout(timeout)
	if err := client.SetProxy(proxy); err != nil {
		slog.Debug("proxy preflight set proxy failed", "proxy", proxy, "err", err)
		return false
	}
	headers := httpclient.AuroraHeaders{}
	headers.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	headers.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36")
	response, err := client.Request(http.MethodGet, "https://chatgpt.com/", headers, nil, nil)
	if err != nil {
		slog.Debug("proxy preflight failed", "proxy", proxy, "err", err)
		return false
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1024))
	if response.StatusCode >= 200 && response.StatusCode < 500 {
		return true
	}
	slog.Debug("proxy preflight returned bad status", "proxy", proxy, "status", response.StatusCode)
	return false
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
