package main

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

var (
	imageExts  = []string{".gif", ".png", ".jpg", ".jpeg", ".webp"}
	httpClient = &http.Client{Timeout: 12 * time.Second}
)

const liveTTL = 15 * time.Minute

type User struct {
	XMLName xml.Name `xml:"user" json:"-"`
	ID      int      `xml:"id" json:"id"`
	URL     string   `xml:"url" json:"url"`
	Name    string   `xml:"name" json:"name"`
	Image   string   `xml:"-" json:"image,omitempty"`
}

type Users struct {
	XMLName xml.Name `xml:"users"`
	Users   []User   `xml:"user"`
}

type Store struct {
	path  string
	mu    sync.RWMutex
	users []User
}

type ReviewResult struct {
	OK    bool   `json:"ok"`
	URL   string `json:"url,omitempty"`
	Name  string `json:"name,omitempty"`
	Image string `json:"image,omitempty"`
	User  *User  `json:"user,omitempty"`
}

type LiveSite struct {
	URL      string    `xml:"url" json:"url"`
	LastSeen time.Time `xml:"lastSeen" json:"lastSeen"`
}

type liveFile struct {
	XMLName xml.Name   `xml:"live"`
	Sites   []LiveSite `xml:"site"`
}

type Live struct {
	path string
	mu   sync.RWMutex
	by   map[string]LiveSite
}

type bucket struct {
	mu sync.Mutex
	t  []time.Time
}

type Limiter struct {
	mu    sync.Mutex
	keys  map[string]*bucket
	joins map[string]*bucket
	global *bucket
}

type Discord struct {
	url    string
	client *http.Client
}

func main() {
	loadDotEnv(".env")
	gin.SetMode(gin.ReleaseMode)

	store, err := openStore("static/users.xml")
	if err != nil {
		log.Fatal(err)
	}
	live := openLive("static/live.xml")
	limit := newLimiter()
	hook := &Discord{url: os.Getenv("WEBHOOK"), client: &http.Client{Timeout: 8 * time.Second}}
	addr := env("ADDR", ":3400")

	r := gin.New()
	r.Use(gin.Recovery(), cors)
	r.Static("/static", "./static")
	r.StaticFile("/embed.js", "./static/embed.js")
	r.StaticFile("/stream.js", "./static/stream.js")
	mount(r, store, live, limit, hook)

	go func() {
		for range time.Tick(5 * time.Minute) {
			live.pull(store)
		}
	}()

	log.Printf("listening %s members=%d", addr, store.count())
	log.Fatal(r.Run(addr))
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func loadDotEnv(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		v = strings.Trim(v, `"'`)
		if os.Getenv(k) == "" {
			_ = os.Setenv(k, v)
		}
	}
}

func cors(c *gin.Context) {
	c.Header("Access-Control-Allow-Origin", "*")
	c.Header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	c.Header("Access-Control-Allow-Headers", "Content-Type")
	if c.Request.Method == http.MethodOptions {
		c.AbortWithStatus(http.StatusNoContent)
		return
	}
	c.Next()
}

func empty(c *gin.Context, code int) {
	c.Status(code)
	c.Writer.WriteHeaderNow()
}

func openStore(path string) (*Store, error) {
	s := &Store{path: path}
	return s, s.reload()
}

func (s *Store) reload() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return err
	}
	var root Users
	if err := xml.Unmarshal(data, &root); err != nil {
		return err
	}
	s.mu.Lock()
	s.users = root.Users
	s.mu.Unlock()
	return nil
}

func (s *Store) write() error {
	data, err := xml.MarshalIndent(Users{Users: s.users}, "", "  ")
	if err != nil {
		return err
	}
	out := append([]byte(xml.Header), data...)
	out = append(out, '\n')
	return os.WriteFile(s.path, out, 0644)
}

func (s *Store) all() []User {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]User, len(s.users))
	for i, u := range s.users {
		out[i] = attachImage(u)
	}
	return out
}

func (s *Store) count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.users)
}

func (s *Store) byID(id int) (*User, int, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i, u := range s.users {
		if u.ID == id {
			u = attachImage(u)
			return &u, i, true
		}
	}
	return nil, -1, false
}

func (s *Store) byIndex(i int) (*User, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := len(s.users)
	if n == 0 {
		return nil, false
	}
	i = ((i % n) + n) % n
	u := attachImage(s.users[i])
	return &u, true
}

func (s *Store) neighbor(id, delta int) (*User, bool) {
	_, i, ok := s.byID(id)
	if !ok {
		return nil, false
	}
	return s.byIndex(i + delta)
}

func (s *Store) nextID() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	max := 0
	for _, u := range s.users {
		if u.ID > max {
			max = u.ID
		}
	}
	return max + 1
}

func (s *Store) has(site string) bool {
	n := norm(site)
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, u := range s.users {
		if norm(u.URL) == n {
			return true
		}
	}
	return false
}

func (s *Store) find(site string) *User {
	n := norm(site)
	for _, u := range s.all() {
		if norm(u.URL) == n {
			return &u
		}
	}
	return nil
}

func (s *Store) add(u User) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.users = append(s.users, u)
	return s.write()
}

func (s *Store) xml() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, err := xml.MarshalIndent(Users{Users: s.users}, "", "  ")
	if err != nil {
		return ""
	}
	return xml.Header + string(data) + "\n"
}

func (s *Store) join(site string) (*User, bool) {
	res := review(site)
	if !res.OK || s.has(res.URL) {
		return nil, false
	}
	id := s.nextID()
	if strings.HasPrefix(res.Image, "http") {
		_ = saveImage(id, res.Image)
	}
	u := User{ID: id, URL: res.URL, Name: res.Name}
	if s.add(u) != nil {
		return nil, false
	}
	ou := attachImage(u)
	return &ou, true
}

func attachImage(u User) User {
	base := strconv.Itoa(u.ID)
	for _, ext := range imageExts {
		p := filepath.Join("static", "users", base+ext)
		if _, err := os.Stat(p); err == nil {
			u.Image = "/static/users/" + base + ext
			return u
		}
	}
	return u
}

func openLive(path string) *Live {
	l := &Live{path: path, by: map[string]LiveSite{}}
	data, err := os.ReadFile(path)
	if err != nil {
		return l
	}
	var root liveFile
	if xml.Unmarshal(data, &root) != nil {
		return l
	}
	now := time.Now()
	for _, s := range root.Sites {
		if now.Sub(s.LastSeen) <= liveTTL {
			l.by[norm(s.URL)] = s
		}
	}
	return l
}

func (l *Live) save() {
	root := liveFile{}
	for _, s := range l.by {
		root.Sites = append(root.Sites, s)
	}
	data, err := xml.MarshalIndent(root, "", "  ")
	if err != nil {
		return
	}
	out := append([]byte(xml.Header), data...)
	out = append(out, '\n')
	_ = os.WriteFile(l.path, out, 0644)
}

func (l *Live) touch(site string) LiveSite {
	site = strings.TrimRight(strings.TrimSpace(site), "/")
	s := LiveSite{URL: site, LastSeen: time.Now().UTC()}
	l.mu.Lock()
	l.by[norm(site)] = s
	l.save()
	l.mu.Unlock()
	return s
}

func (l *Live) list() []LiveSite {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	out := make([]LiveSite, 0, len(l.by))
	for k, s := range l.by {
		if now.Sub(s.LastSeen) > liveTTL {
			delete(l.by, k)
			continue
		}
		out = append(out, s)
	}
	l.save()
	return out
}

func (l *Live) pull(store *Store) {
	for _, s := range l.list() {
		if store.has(s.URL) {
			continue
		}
		store.join(s.URL)
	}
}

func newLimiter() *Limiter {
	return &Limiter{
		keys:   map[string]*bucket{},
		joins:  map[string]*bucket{},
		global: &bucket{},
	}
}

func (b *bucket) hit(max int, window time.Duration) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	cut := time.Now().Add(-window)
	keep := b.t[:0]
	for _, t := range b.t {
		if t.After(cut) {
			keep = append(keep, t)
		}
	}
	b.t = keep
	if len(b.t) >= max {
		return false
	}
	b.t = append(b.t, time.Now())
	return true
}

func (l *Limiter) key(m map[string]*bucket, k string) *bucket {
	l.mu.Lock()
	defer l.mu.Unlock()
	if b, ok := m[k]; ok {
		return b
	}
	b := &bucket{}
	m[k] = b
	return b
}

func (l *Limiter) connect(ip, site string) bool {
	if ip == "" {
		ip = "unknown"
	}
	return l.key(l.keys, "ip:"+ip).hit(6, time.Minute) &&
		l.key(l.keys, "site:"+norm(site)).hit(3, time.Minute)
}

func (l *Limiter) join(ip string) bool {
	if ip == "" {
		ip = "unknown"
	}
	return l.key(l.joins, ip).hit(2, time.Hour) && l.global.hit(20, time.Hour)
}

func ipOf(r *http.Request) string {
	if x := r.Header.Get("X-Forwarded-For"); x != "" {
		return strings.TrimSpace(strings.Split(x, ",")[0])
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func publicURL(raw string) (string, bool) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.User != nil || u.Host == "" {
		return "", false
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", false
	}
	host := strings.ToLower(u.Hostname())
	if host == "localhost" || strings.HasSuffix(host, ".localhost") || host == "metadata.google.internal" {
		return "", false
	}
	if ip := net.ParseIP(host); ip != nil && !safeIP(ip) {
		return "", false
	}
	if ips, err := net.LookupIP(host); err == nil {
		for _, ip := range ips {
			if !safeIP(ip) {
				return "", false
			}
		}
	}
	u.Path = strings.TrimRight(u.Path, "/")
	u.RawQuery, u.Fragment = "", ""
	return u.String(), true
}

func safeIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast() {
		return false
	}
	if v4 := ip.To4(); v4 != nil && v4[0] == 169 && v4[1] == 254 {
		return false
	}
	return true
}

func (d *Discord) send(v any) {
	if d == nil || d.url == "" {
		return
	}
	go func() {
		raw, err := json.Marshal(v)
		if err != nil {
			return
		}
		msg := string(raw)
		if len(msg) > 1900 {
			msg = msg[:1900]
		}
		body, _ := json.Marshal(map[string]string{"content": msg})
		req, err := http.NewRequest(http.MethodPost, d.url, bytes.NewReader(body))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := d.client.Do(req)
		if err != nil {
			return
		}
		resp.Body.Close()
	}()
}

func review(site string) ReviewResult {
	res := ReviewResult{URL: site}
	site = strings.TrimSpace(site)
	base, err := url.Parse(site)
	if err != nil || (base.Scheme != "http" && base.Scheme != "https") {
		return res
	}
	base.Path = strings.TrimRight(base.Path, "/")
	res.URL = base.String()

	body, err := httpGet(res.URL + "/snowcats.json")
	if err != nil || looksHTML(body) {
		return res
	}

	var raw map[string]any
	if json.Unmarshal(body, &raw) != nil {
		return res
	}
	name, _ := raw["name"].(string)
	image, _ := raw["image"].(string)
	name, image = strings.TrimSpace(name), strings.TrimSpace(image)
	if name == "" || image == "" {
		return res
	}
	for k := range raw {
		if k != "name" && k != "image" {
			return res
		}
	}
	res.Name, res.Image = name, image
	if abs, err := resolve(base, image); err == nil {
		if okImg, ok := checkImage(abs); ok {
			res.Image = okImg
		}
	}
	res.OK = true
	return res
}

func looksHTML(b []byte) bool {
	t := bytes.TrimSpace(b)
	return bytes.HasPrefix(t, []byte("<")) || bytes.HasPrefix(t, []byte("<!"))
}

func checkImage(imgURL string) (string, bool) {
	body, err := httpGet(imgURL)
	if err != nil || looksHTML(body) {
		return "", false
	}
	cfg, _, err := image.DecodeConfig(bytes.NewReader(body))
	if err != nil || cfg.Width == 0 || cfg.Height == 0 {
		return "", false
	}
	ratio := float64(cfg.Width) / float64(cfg.Height)
	if ratio < 1.45 || ratio > 1.55 {
		return "", false
	}
	return imgURL, true
}

func saveImage(id int, imgURL string) error {
	body, err := httpGet(imgURL)
	if err != nil {
		return err
	}
	ext := ".png"
	if u, err := url.Parse(imgURL); err == nil {
		e := strings.ToLower(filepath.Ext(u.Path))
		for _, ok := range imageExts {
			if e == ok {
				ext = e
				break
			}
		}
	}
	dir := filepath.Join("static", "users")
	_ = os.MkdirAll(dir, 0755)
	return os.WriteFile(filepath.Join(dir, strconv.Itoa(id)+ext), body, 0644)
}

func resolve(base *url.URL, ref string) (string, error) {
	r, err := url.Parse(ref)
	if err != nil {
		return "", err
	}
	return base.ResolveReference(r).String(), nil
}

func httpGet(raw string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, raw, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "snowcats/1.0")
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 8<<20))
}

func norm(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return strings.ToLower(strings.TrimRight(raw, "/"))
	}
	host := strings.ToLower(strings.TrimPrefix(u.Hostname(), "www."))
	return host + strings.TrimRight(u.Path, "/")
}

func readURL(c *gin.Context) string {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 512)
	var body struct {
		URL string `json:"url"`
	}
	_ = c.ShouldBindJSON(&body)
	if body.URL == "" {
		body.URL = c.Query("url")
	}
	return strings.TrimSpace(body.URL)
}

func mount(r *gin.Engine, store *Store, live *Live, limit *Limiter, hook *Discord) {
	api := r.Group("/api")

	api.GET("/load", func(c *gin.Context) {
		c.JSON(200, gin.H{"members": store.all(), "count": store.count()})
	})

	api.GET("/random", func(c *gin.Context) {
		all := store.all()
		if len(all) == 0 {
			empty(c, 404)
			return
		}
		c.JSON(200, all[rand.Intn(len(all))])
	})

	api.GET("/next/:id", func(c *gin.Context) {
		id, err := strconv.Atoi(c.Param("id"))
		if err != nil {
			empty(c, 400)
			return
		}
		u, ok := store.neighbor(id, 1)
		if !ok {
			empty(c, 404)
			return
		}
		c.JSON(200, u)
	})

	api.GET("/back/:id", func(c *gin.Context) {
		id, err := strconv.Atoi(c.Param("id"))
		if err != nil {
			empty(c, 400)
			return
		}
		u, ok := store.neighbor(id, -1)
		if !ok {
			empty(c, 404)
			return
		}
		c.JSON(200, u)
	})

	api.POST("/connect", func(c *gin.Context) {
		site := readURL(c)
		if site == "" {
			empty(c, 400)
			return
		}
		ip := ipOf(c.Request)
		if !limit.connect(ip, site) {
			empty(c, 429)
			return
		}
		clean, ok := publicURL(site)
		if !ok {
			empty(c, 400)
			return
		}
		site = clean

		if store.has(site) {
			s := live.touch(site)
			u := store.find(site)
			if u == nil {
				empty(c, 400)
				return
			}
			c.JSON(200, gin.H{"status": "member", "url": s.URL, "user": u})
			return
		}
		if !limit.join(ip) {
			empty(c, 429)
			return
		}
		u, ok := store.join(site)
		if !ok {
			empty(c, 400)
			return
		}
		s := live.touch(site)
		hook.send(map[string]any{"event": "joined", "user": u, "users_xml": store.xml()})
		c.JSON(201, gin.H{"status": "joined", "url": s.URL, "user": u})
	})
}
