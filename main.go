package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/xerrors"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const BUFFER_SIZE = 4096

var variants = []string{
	"chess", "antichess", "atomic", "crazyhouse", "horde", "kingofthehill", "racingkings", "3check",
}

var validLogLevels = []string{
	"debug", "info", "warn", "error", "panic", "fatal",
}

type Args struct {
	Name           string
	Engine         string
	Lichess        string
	Broker         string
	Token          string
	ProviderSecret string
	DefaultDepth   int
	MinThreads     int
	MaxThreads     int
	MinHash        int
	MaxHash        int
	MaxMoveTime    int
	KeepAlive      int
	LogLevel       string
}

var args Args

type Engine struct {
	Process           *exec.Cmd
	SessionID         string
	Threads           int
	Hash              int
	MultiPV           int
	UCIVariant        string
	SupportedVariants []string
	lastUsedEpoch     int64
	isAlive           int64
	stopLock          sync.Mutex
	analysisLock      sync.Mutex
	Stdin             *bufio.Writer
	Stdout            *bufio.Scanner

	readStream chan string
}

type AnalysisRequest struct {
	ID     string         `json:"id"`
	Work   AnalysisWork   `json:"work"`
	Engine AnalysisEngine `json:"engine"`
}

type AnalysisWork struct {
	SessionID  string   `json:"sessionId"`
	Threads    int      `json:"threads"`
	Hash       int      `json:"hash"`
	MoveTime   int      `json:"movetime"`
	MultiPV    int      `json:"multiPv"`
	Variant    string   `json:"variant"`
	InitialFEN string   `json:"initialFen"`
	Moves      []string `json:"moves"`
}

type AnalysisEngine struct {
	ID           string   `json:"id"`
	Name         string   `json:"name"`
	ClientSecret string   `json:"clientSecret"`
	UserID       string   `json:"userId"`
	MaxThreads   int      `json:"maxThreads"`
	MaxHash      int      `json:"maxHash"`
	Variants     []string `json:"variants"`
	ProviderData string   `json:"providerData"`
}

func NewEngine(command string) (*Engine, error) {
	ctx := context.Background()

	cmd := exec.CommandContext(ctx, command)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, xerrors.Errorf("error creating stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, xerrors.Errorf("error creating stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return nil, xerrors.Errorf("error executing cmd.Start() for '%s': %w", command, err)
	}

	engine := &Engine{
		Process:           cmd,
		Threads:           args.MaxThreads,
		Hash:              args.MaxHash,
		lastUsedEpoch:     time.Now().Unix(),
		isAlive:           1,
		Stdin:             bufio.NewWriter(stdin),
		Stdout:            bufio.NewScanner(stdout),
		SupportedVariants: []string{},
		readStream:        make(chan string, BUFFER_SIZE),
	}

	go func() {
		if err := engine.recv(); err != nil {
			zap.L().Fatal("error reading from engine", zap.Error(err))
		}
		zap.L().Info("engine exited")
		atomic.StoreInt64(&engine.isAlive, 0)
	}()

	if err := engine.uci(); err != nil {
		return nil, xerrors.Errorf("error initializing uci: %w", err)
	}

	_ = engine.setOption("000", "UCI_AnalyseMode", "true")
	_ = engine.setOption("000", "UCI_Chess960", "true")
	_ = engine.setOption("000", "UCI_Variant", "chess")
	_ = engine.setOption("000", "Hash", fmt.Sprintf("%d", args.MaxHash))
	_ = engine.setOption("000", "Threads", fmt.Sprintf("%d", args.MaxThreads))

	go func() {
		if err := cmd.Wait(); err != nil {
			if err.Error() == "signal: killed" {
				return
			}
			zap.L().Fatal("cmd.Wait(): abnormal termination", zap.Error(err), zap.String("command", command))
		}
	}()

	return engine, nil
}

func (e *Engine) idleTime() time.Duration {
	diff := time.Now().Unix() - atomic.LoadInt64(&e.lastUsedEpoch)
	return time.Duration(diff) * time.Second
}

func (e *Engine) terminate() error {
	if !atomic.CompareAndSwapInt64(&e.isAlive, 1, 0) {
		return nil
	}

	if err := e.Process.Process.Kill(); err != nil {
		return xerrors.Errorf("failed to terminate engine process: %w", err)
	}
	return nil
}

func (e *Engine) send(command string) error {
	now := time.Now().Unix()
	atomic.StoreInt64(&e.lastUsedEpoch, now)

	_, err := e.Stdin.WriteString(command + "\n")
	if err != nil {
		return xerrors.Errorf("error writing to engine, command: '%s', error: %w", command, err)
	}
	if err := e.Stdin.Flush(); err != nil {
		return xerrors.Errorf("error flushing command to engine, command: '%s', error: %w", command, err)
	}
	zap.L().Debug("sent command to engine", zap.String("command", command))
	return nil
}

func (e *Engine) recv() error {
	for e.Stdout.Scan() {
		line := e.Stdout.Text()
		zap.L().Debug("received from engine", zap.String("line", line))
		e.readStream <- line
	}
	if err := e.Stdout.Err(); err != nil {
		return xerrors.Errorf("stdout: %w", err)
	}
	return nil
}

func (e *Engine) uci() error {
	if err := e.send("uci"); err != nil {
		return xerrors.Errorf("%w", err)
	}

	for line := range e.readStream {
		parts := strings.Split(line, " ")
		command := parts[0]
		if command == "id" {
			if len(parts) >= 3 && parts[1] == "name" && args.Name == "" {
				args.Name = fmt.Sprintf("External %s", parts[2])
			}
		} else if command == "option" {
			for i := 1; i < len(parts); i++ {
				if parts[i] == "name" && i+1 < len(parts) {
					name := parts[i+1]
					i++
					if name == "UCI_Variant" && i+1 < len(parts) && parts[i+1] == "var" {
						i++
						e.SupportedVariants = append(e.SupportedVariants, parts[i])
					}
				}
			}
		} else if command == "uciok" {
			break
		}
	}

	if len(e.SupportedVariants) > 0 {
		zap.L().Info("supported variants", zap.Strings("variants", e.SupportedVariants))
	} else {
		e.SupportedVariants = append(e.SupportedVariants, "chess")
	}

	return nil
}

func (e *Engine) isReady(jobID string) error {
	zap.L().Debug("sending isready", zap.String("job_id", jobID))

	if err := e.send("isready"); err != nil {
		return xerrors.Errorf("%w", err)
	}

	for line := range e.readStream {
		if line == "readyok" {
			zap.L().Debug("readyok received", zap.String("job_id", jobID))
			break
		}
	}
	return nil
}

func (e *Engine) setOption(jobID string, name, value string) error {
	cmd := fmt.Sprintf("setoption name %s value %s", name, value)
	zap.L().Info("sent to engine", zap.String("job_id", jobID), zap.String("command", cmd))
	return e.send(cmd)
}

func (e *Engine) analyse(ctx context.Context, job AnalysisRequest, jobStarted chan struct{}) (<-chan string, error) {
	zap.L().Info("handling job - before lock", zap.String("job_id", job.ID))

	e.analysisLock.Lock()

	work := job.Work
	jobID := job.ID

	zap.L().Info("handling job - after lock", zap.String("job_id", job.ID))

	if work.SessionID != e.SessionID {
		e.SessionID = work.SessionID
		zap.L().Info("sent to engine", zap.String("job_id", jobID), zap.String("command", "ucinewgame"))
		if err := e.send("ucinewgame"); err != nil {
			e.analysisLock.Unlock()
			return nil, xerrors.Errorf("%w", err)
		}
		if err := e.isReady(jobID); err != nil {
			e.analysisLock.Unlock()
			return nil, xerrors.Errorf("%w", err)
		}
	}

	optionsChanged := false
	if e.Hash != work.Hash {
		if err := e.setOption(jobID, "Hash", strconv.Itoa(work.Hash)); err != nil {
			e.analysisLock.Unlock()
			return nil, xerrors.Errorf("%w", err)
		}
		e.Hash = work.Hash
		optionsChanged = true
	}
	if e.Threads != work.Threads {
		if err := e.setOption(jobID, "Threads", strconv.Itoa(work.Threads)); err != nil {
			e.analysisLock.Unlock()
			return nil, xerrors.Errorf("%w", err)
		}
		e.Threads = work.Threads
		optionsChanged = true
	}
	if e.MultiPV != work.MultiPV {
		if err := e.setOption(jobID, "MultiPV", strconv.Itoa(work.MultiPV)); err != nil {
			e.analysisLock.Unlock()
			return nil, xerrors.Errorf("%w", err)
		}
		e.MultiPV = work.MultiPV
		optionsChanged = true
	}
	if e.UCIVariant != work.Variant {
		if err := e.setOption(jobID, "UCI_Variant", work.Variant); err != nil {
			e.analysisLock.Unlock()
			return nil, xerrors.Errorf("%w", err)
		}
		e.UCIVariant = work.Variant
		optionsChanged = true
	}

	if optionsChanged {
		if err := e.isReady(jobID); err != nil {
			e.analysisLock.Unlock()
			return nil, xerrors.Errorf("%w", err)
		}
	}

	var uciPositionCommand string

	if len(work.Moves) == 0 {
		uciPositionCommand = fmt.Sprintf("position fen %s", work.InitialFEN)
	} else {
		uciPositionCommand = fmt.Sprintf("position fen %s moves %s",
			work.InitialFEN,
			strings.Join(work.Moves, " "),
		)
	}

	if err := e.send(uciPositionCommand); err != nil {
		e.analysisLock.Unlock()
		return nil, xerrors.Errorf("%w", err)
	}
	zap.L().Info("sent to engine", zap.String("job_id", jobID), zap.String("command", uciPositionCommand))
	if err := e.isReady(jobID); err != nil {
		e.analysisLock.Unlock()
		return nil, xerrors.Errorf("%w", err)
	}

	var goCommand string
	if work.MoveTime > 0 && work.MoveTime <= args.MaxMoveTime {
		goCommand = fmt.Sprintf("go movetime %d", work.MoveTime)
	} else {
		goCommand = fmt.Sprintf("go depth %d movetime %d", args.DefaultDepth, args.MaxMoveTime)
	}
	zap.L().Info("sent to engine", zap.String("job_id", jobID), zap.String("command", goCommand))

	if err := e.send(goCommand); err != nil {
		e.analysisLock.Unlock()
		return nil, xerrors.Errorf("%w", err)
	}
	jobStarted <- struct{}{}

	analysisStream := make(chan string, BUFFER_SIZE)
	go func() {
		defer func() {
			zap.L().Info("streamAnalysis has ended", zap.String("job_id", jobID))
			e.analysisLock.Unlock()
		}()
		if err := e.streamAnalysis(ctx, jobID, analysisStream); err != nil {
			zap.L().Warn("error streaming analysis", zap.String("job_id", jobID), zap.Error(err))
		}
	}()

	return analysisStream, nil
}

func (e *Engine) streamAnalysis(ctx context.Context, jobID string, analysisStream chan<- string) (returnError error) {
	zap.L().Info("starting analysis", zap.String("job_id", jobID))

	defer func() {
		if errors.Is(returnError, context.Canceled) {
			zap.L().Info("streamAnalysis: context canceled", zap.String("job_id", jobID))
			returnError = nil
		}

		close(analysisStream)

		zap.L().Info("analysis ended", zap.String("job_id", jobID))
	}()

	for {
		select {
		case <-ctx.Done():
			zap.L().Info("streamAnalysis end: context canceled", zap.String("job_id", jobID))
			if err := e.stop(jobID); err != nil {
				returnError = err
				return
			}
			zap.L().Info("streamAnalysis end: stop sent", zap.String("job_id", jobID))
			for line := range e.readStream {
				parts := strings.Split(line, " ")
				command := parts[0]
				if command == "bestmove" {
					zap.L().Info("streamAnalysis end: bestmove consumed", zap.String("job_id", jobID))
					return
				} else {
					zap.L().Info("streamAnalysis end: discarded line", zap.String("job_id", jobID), zap.String("line", line))
				}
			}
			return
		case line := <-e.readStream:
			parts := strings.Split(line, " ")
			command := parts[0]

			if command == "bestmove" {
				zap.L().Info("bestmove received", zap.String("job_id", jobID), zap.String("line", line))

				if err := e.stop(jobID); err != nil {
					returnError = err
					return
				}

				if err := e.isReady(jobID); err != nil {
					returnError = err
					return
				}
				return
			}

			if command == "info" {
				if !strings.Contains(line, "score") {
					continue
				}

				analysisStream <- line

				zap.L().Info("engine output", zap.String("job_id", jobID), zap.String("line", line))
			} else {
				zap.L().Warn("readStream: unexpected engine command", zap.String("job_id", jobID), zap.String("command", command))
			}
		}
	}
}

func (e *Engine) stop(jobID string) error {
	if atomic.LoadInt64(&e.isAlive) == 0 {
		return nil
	}

	e.stopLock.Lock()
	defer e.stopLock.Unlock()

	zap.L().Info("sending stop", zap.String("job_id", jobID))
	if err := e.send("stop"); err != nil {
		return xerrors.Errorf("%w", err)
	}
	return nil
}

func ok(res *http.Response, doErr error) (*http.Response, error) {
	if doErr != nil {
		return res, xerrors.Errorf("http request failed: %w", doErr)
	}
	if res.StatusCode >= 400 {
		return res, xerrors.Errorf("http response error: %d %s", res.StatusCode, res.Status)
	}
	return res, nil
}

// tokenURLSafe generates a random URL-safe text string in Base64 encoding.
// The string has n random bytes.
func tokenURLSafe(n int) (string, error) {
	if n <= 0 {
		panic(fmt.Errorf("n must be > 0. n = %d", n))
	}

	// Generate random bytes
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		return "", xerrors.Errorf("%w", err)
	}

	// Encode to URL-safe Base64
	s := base64.URLEncoding.EncodeToString(b)
	s = strings.TrimRight(s, "=")

	return s, nil
}

func newHTTPRequest(method, url string, body io.Reader) *http.Request {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		zap.L().Fatal("failed to create http request", zap.Error(err), zap.String("method", method), zap.String("url", url))
	}
	if method == "POST" || method == "PUT" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", args.Token))
	return req
}

type EngineRegistration struct {
	Name           string   `json:"name"`
	MinThreads     int      `json:"minThreads"`
	MaxThreads     int      `json:"maxThreads"`
	MaxHash        int      `json:"maxHash"`
	MaxMoveTime    int      `json:"maxMoveTime"`
	DefaultDepth   int      `json:"defaultDepth"`
	Variants       []string `json:"variants,omitempty"`
	ProviderSecret string   `json:"providerSecret"`
	ProviderData   string   `json:"providerData,omitempty"`
}

func registerEngine(engine *Engine) (string, error) {
	// Get existing engines
	externalEngineEndpointURL := fmt.Sprintf("%s/api/external-engine", args.Lichess)

	req := newHTTPRequest("GET", externalEngineEndpointURL, nil)

	res, err := ok(http.DefaultClient.Do(req))
	respBody, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return "", xerrors.Errorf("error during GET %s: %w, body: '%s'", externalEngineEndpointURL, err, string(respBody))
	}

	// Provider Secret
	secret := args.ProviderSecret
	if secret == "" {
		// Generate a new secret
		secret, err = tokenURLSafe(32)
		if err != nil {
			return "", xerrors.Errorf("error generating secret: %w", err)
		}
	}

	// Define registration data
	registration := EngineRegistration{
		Name:           args.Name,
		MinThreads:     args.MinThreads,
		MaxThreads:     args.MaxThreads,
		MaxHash:        args.MaxHash,
		MaxMoveTime:    args.MaxMoveTime,
		DefaultDepth:   args.DefaultDepth,
		Variants:       filterVariants(engine.SupportedVariants, variants),
		ProviderSecret: secret,
	}

	// Decode response
	var existingEngines []map[string]interface{}
	if err := json.Unmarshal(respBody, &existingEngines); err != nil {
		return "", xerrors.Errorf("error parsing response from GET %s: %w", externalEngineEndpointURL, err)
	}

	// Check if engine already exists
	for _, e := range existingEngines {
		if e["name"] == args.Name {
			engineID := e["id"].(string)
			zap.L().Info("updating engine", zap.String("engine_id", engineID))

			// Update engine
			updateEngineEndpointURL := fmt.Sprintf("%s/api/external-engine/%s", args.Lichess, engineID)

			jsonData, err := json.Marshal(registration)
			if err != nil {
				zap.L().Fatal("error marshalling registration data", zap.Error(err))
			}

			req := newHTTPRequest("PUT", updateEngineEndpointURL, bytes.NewBuffer(jsonData))
			if _, err := ok(http.DefaultClient.Do(req)); err != nil {
				return "", xerrors.Errorf("error during PUT %s: %w", updateEngineEndpointURL, err)
			}
			return secret, nil
		}
	}

	// Register new engine
	createEngineEndpointURL := fmt.Sprintf("%s/api/external-engine", args.Lichess)

	zap.L().Info("registering new engine")
	jsonData, err := json.Marshal(registration)
	if err != nil {
		return "", xerrors.Errorf("error marshalling registration data: %w", err)
	}
	req = newHTTPRequest("POST", createEngineEndpointURL, bytes.NewBuffer(jsonData))
	if resp, err := ok(http.DefaultClient.Do(req)); err != nil {
		respBody, _ := io.ReadAll(resp.Body)
		return "", xerrors.Errorf("error during POST %s: %w, body: %s payload: %s", createEngineEndpointURL, err, respBody, jsonData)
	}
	return secret, nil
}

func filterVariants(supported, allowed []string) []string {
	var filtered []string
	for _, v := range supported {
		for _, a := range allowed {
			if v == a {
				filtered = append(filtered, v)
				break
			}
		}
	}
	return filtered
}

func main() {
	var showVersion bool

	flag.BoolVar(&showVersion, "version", false, "Show version and exit")
	flag.StringVar(&args.Name, "name", "", "Engine name to register")
	flag.StringVar(&args.Engine, "engine", os.Getenv("UCI_ENGINE_PATH"), "Shell command to launch UCI engine")
	flag.StringVar(&args.Lichess, "lichess", "https://lichess.org", "Defaults to https://lichess.org")
	flag.StringVar(&args.Broker, "broker", "https://engine.lichess.ovh", "Defaults to https://engine.lichess.ovh")
	flag.StringVar(&args.Token, "token", os.Getenv("LICHESS_API_TOKEN"), "API token with engine:read and engine:write scopes")
	flag.StringVar(&args.ProviderSecret, "provider-secret", os.Getenv("PROVIDER_SECRET"), "Optional fixed provider secret")
	flag.IntVar(&args.DefaultDepth, "default-depth", 30, "Default search depth")
	flag.IntVar(&args.MinThreads, "min-threads", 4, "Minimum number of available threads")
	flag.IntVar(&args.MaxThreads, "max-threads", runtime.NumCPU(), "Maximum number of available threads")
	flag.IntVar(&args.MinHash, "min-hash", 512, "Minimum hash table size in MiB")
	flag.IntVar(&args.MaxHash, "max-hash", 4096, "Maximum hash table size in MiB")
	flag.IntVar(&args.MaxMoveTime, "max-move-time", 60000, "Maximum move time in milliseconds")
	flag.IntVar(&args.KeepAlive, "keep-alive", 7200, "Number of seconds to keep an idle/unused engine process around")
	flag.StringVar(&args.LogLevel, "log-level", "info", fmt.Sprintf("Logging verbosity (%s)", strings.Join(validLogLevels, ", ")))

	flag.Parse()

	if showVersion {
		info, ok := debug.ReadBuildInfo()
		if !ok {
			fmt.Println("Could not read build info")
			os.Exit(1)
		}
		var (
			revision   string
			lastCommit time.Time
			dirtyBuild bool
		)

		for _, kv := range info.Settings {
			switch kv.Key {
			case "vcs.revision":
				revision = kv.Value
			case "vcs.time":
				lastCommit, _ = time.Parse(time.RFC3339, kv.Value)
			case "vcs.modified":
				dirtyBuild = kv.Value == "true"
			}
		}

		fmt.Printf("Revision:      %s\n", revision)
		fmt.Printf("Last Commit:   %s\n", lastCommit.Format(time.RFC3339))
		fmt.Printf("Dirty Build:   %t\n", dirtyBuild)
		fmt.Printf("Go Version:    %s\n", info.GoVersion)
		fmt.Println()

		os.Exit(0)
	}

	if args.Engine == "" {
		flag.PrintDefaults()
		fmt.Println("The --engine flag is required.")
		os.Exit(128)
	}

	if args.Token == "" {
		fmt.Printf("Need LICHESS_API_TOKEN environment variable from %s/account/oauth/token/create?scopes[]=engine:read&scopes[]=engine:write\n", args.Lichess)
		os.Exit(128)
	}

	configureLogging(args.LogLevel)

	engine, err := NewEngine(args.Engine)
	if err != nil {
		//
		zap.L().Fatal("failed to initialize engine", zap.Error(err))
	}
	if args.Name == "" {
		args.Name = "External Engine"
	}
	secret, err := registerEngine(engine)
	if err != nil {
		zap.L().Fatal("failed to register engine", zap.Error(err))
	}

	obj := struct {
		ProviderSecret string `json:"providerSecret"`
	}{ProviderSecret: secret}

	providerSecretJSON, err := json.Marshal(obj)
	if err != nil {
		zap.L().Fatal("failed to marshal json", zap.Error(err))
	}

	var queueSize int64
	queue := make(chan AnalysisRequest, BUFFER_SIZE)
	go func() {
		var (
			prevCtxCancel context.CancelFunc
			prevJobID     string
		)

		for job := range queue {
			size := atomic.AddInt64(&queueSize, -1)
			if size != 0 {
				zap.L().Info("skipping job", zap.String("job_id", job.ID), zap.Int64("queue_size", size))
				continue
			}

			if atomic.LoadInt64(&engine.isAlive) == 0 {
				engine, err = NewEngine(args.Engine)
				if err != nil {
					zap.L().Fatal("failed to reinitialize engine", zap.Error(err))
				}
			}

			if prevCtxCancel != nil {
				zap.L().Info("1. cancelling previous work request", zap.String("new_job_id", job.ID), zap.String("old_job_id", prevJobID))
				prevCtxCancel()
				zap.L().Info("2. cancelling previous work request (called)", zap.String("new_job_id", job.ID), zap.String("old_job_id", prevJobID))
			}

			ctx, cancel := context.WithCancel(context.Background())
			prevCtxCancel = cancel

			jobStarted := make(chan struct{})
			go handleJob(ctx, engine, job, jobStarted)
			<-jobStarted
			prevJobID = job.ID
		}
	}()

	backoff := 1.0
	for {
		if backoff > 1.0 {
			zap.L().Info("listening for work requests", zap.Float64("backoff", backoff))
		} else {
			zap.L().Info("listening for work requests")
		}

		bodyReader := bytes.NewReader(providerSecretJSON)

		req := newHTTPRequest("POST", fmt.Sprintf("%s/api/external-engine/work", args.Broker), bodyReader)

		res, err := http.DefaultClient.Do(req)
		if err != nil {
			backoff = min(backoff*1.5, 10)
			sleep := time.Duration(backoff) * time.Second
			zap.L().Error("error while trying to acquire work", zap.Duration("backoff", sleep), zap.Error(err))
			time.Sleep(sleep)
			continue
		}

		if res.StatusCode == 200 || res.StatusCode == 204 {
			backoff = max(1.0, backoff/1.5)
		}

		if res.StatusCode != 200 {
			if res.StatusCode != 204 {
				backoff = min(backoff*1.5, 10)
				sleep := time.Duration(backoff) * time.Second
				zap.L().Error("error while trying to acquire work", zap.Duration("backoff", sleep), zap.Error(err))
			}
			isAlive := atomic.LoadInt64(&engine.isAlive)
			if isAlive == 1 && engine.idleTime() > time.Duration(args.KeepAlive)*time.Second {
				zap.L().Info("terminating idle engine")
				if err := engine.terminate(); err != nil {
					zap.L().Warn("failed to terminate engine", zap.Error(err))
				}
			}
			time.Sleep(time.Duration(backoff) * time.Second)
			continue
		}

		bodyScanner := bufio.NewScanner(res.Body)
		for bodyScanner.Scan() {
			atomic.AddInt64(&queueSize, 1)

			line := bodyScanner.Text()

			var job AnalysisRequest
			if err := json.Unmarshal([]byte(line), &job); err != nil {
				zap.L().Error("failed to unmarshal work request", zap.Error(err), zap.String("json", line))
				break
			}

			job.Work.Hash = max(job.Work.Hash, args.MinHash)
			job.Work.Hash = min(job.Work.Hash, args.MaxHash)

			job.Work.Threads = max(job.Work.Threads, args.MinThreads)
			job.Work.Threads = min(job.Work.Threads, args.MaxThreads)

			zap.L().Info("request from broker",
				zap.String("variant", job.Work.Variant),
				zap.Int("hash", job.Work.Hash),
				zap.Int("threads", job.Work.Threads),
				zap.Int("multipv", job.Work.MultiPV),
				zap.Int("movetime", job.Work.MoveTime),
				zap.String("fen", job.Work.InitialFEN),
				zap.Strings("moves", job.Work.Moves),
			)

			queue <- job
		}
		if err := bodyScanner.Err(); err != nil {
			zap.L().Error("error while reading response from broker", zap.Error(err))
		}
	}
}

func configureLogging(level string) {
	var logConfig zap.Config

	switch level {
	case "debug":
		logConfig = zap.NewDevelopmentConfig()
		logConfig.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	case "info":
		logConfig = zap.NewProductionConfig()
		logConfig.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	case "warn", "warning", "warnings":
		logConfig = zap.NewProductionConfig()
		logConfig.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	case "error", "errors":
		logConfig = zap.NewProductionConfig()
		logConfig.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	case "fatal", "critical", "panic":
		logConfig = zap.NewProductionConfig()
		logConfig.Level = zap.NewAtomicLevelAt(zap.FatalLevel)
	default:
		logConfig = zap.NewProductionConfig()
		logConfig.Level = zap.NewAtomicLevelAt(zap.FatalLevel)

		setGlobalLogger(logConfig)

		zap.L().Fatal("invalid log level",
			zap.Strings("valid_log_levels", validLogLevels),
			zap.String("log_level_arg", level))
	}

	setGlobalLogger(logConfig)
}

func setGlobalLogger(logConfig zap.Config) {
	logEncoderConfig := zap.NewProductionEncoderConfig()
	logEncoderConfig.EncodeTime = zapcore.RFC3339NanoTimeEncoder
	logEncoderConfig.EncodeDuration = zapcore.MillisDurationEncoder

	logConfig.EncoderConfig = logEncoderConfig

	logger, err := logConfig.Build(zap.AddStacktrace(zapcore.ErrorLevel))
	if err != nil {
		panic(err)
	}
	zap.ReplaceGlobals(logger)
}

func handleJob(ctx context.Context, engine *Engine, job AnalysisRequest, jobStarted chan struct{}) {
	defer close(jobStarted)

	zap.L().Info("next up", zap.String("job_id", job.ID))

	analysisStream, err := engine.analyse(ctx, job, jobStarted)
	if err != nil {
		zap.L().Error("error starting analysis", zap.String("job_id", job.ID), zap.Error(err))
		time.Sleep(5 * time.Second)
		return
	}

	submitWorkEndpointURL := fmt.Sprintf("%s/api/external-engine/work/%s", args.Broker, job.ID)

	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		for {
			select {
			case <-ctx.Done():
				zap.L().Info("exiting analysisStream listener loop", zap.String("job_id", job.ID))
				return
			case lineItem, ok := <-analysisStream:
				if !ok {
					zap.L().Info("analysisStream channel closed", zap.String("job_id", job.ID))
					return
				}
				_, err := pw.Write([]byte(lineItem + "\n"))
				if err != nil {
					_ = pw.CloseWithError(err)
					if err.Error() != "io: read/write on closed pipe" {
						zap.L().Error("failed to write to pipe", zap.String("job_id", job.ID), zap.Error(err))
					}
					return
				}
			}
		}
	}()

	req, err := http.NewRequest("POST", submitWorkEndpointURL, pr)
	if err != nil {
		_ = pw.CloseWithError(err)
		zap.L().Fatal("error creating request", zap.String("job_id", job.ID), zap.Error(err))
	}
	req.Header.Set("Content-Type", "text/plain")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		_ = pw.CloseWithError(err)
		zap.L().Error("connection closed while streaming analysis", zap.String("job_id", job.ID), zap.Error(err))
		time.Sleep(5 * time.Second)
		return
	}
	defer resp.Body.Close()

	responseBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		zap.L().Error("error submitting work", zap.String("job_id", job.ID), zap.Int("http_status_code", resp.StatusCode), zap.String("http_status", resp.Status), zap.String("body", string(responseBody)))
		time.Sleep(5 * time.Second)
		return
	}
}

func min[T Number](a, b T) T {
	if a < b {
		return a
	}
	return b
}

func max[T Number](a, b T) T {
	if a > b {
		return a
	}
	return b
}

type Number interface {
	int | float64 | float32
}
