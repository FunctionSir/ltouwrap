/*
 * @Author: FunctionSir
 * @Date: 2025-08-22 11:11:36
 * @LastEditTime: 2025-08-24 13:57:33
 * @LastEditors: FunctionSir
 * @Description: -
 * @FilePath: /ltouwrap/ltouwrap.go
 */

package ltouwrap

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// Debug Mode will show output of tape utils.
var EnableLtoUtilsWrapperDebugMode bool = false

type LtoNoRewindTapeDrive struct {
	DeviceFile string
	SgLogs     string
	SgReadAttr string
	Mt         string
}

type LtoTapeCapacityLog struct {
	MainPartitionRemaining      int64 // In MiB (1 MiB = 1048576 Bytes).
	AlternatePartitionRemaining int64 // In MiB (1 MiB = 1048576 Bytes).
	MainPartitionMax            int64 // In MiB (1 MiB = 1048576 Bytes).
	AlternatePartitionMax       int64 // In MiB (1 MiB = 1048576 Bytes).
}

// A magic number as "Capacity Unknown".
const UnknownCapacity int64 = -1

// In MAM attribute data, 0 means data cartridge, 1 means cleaning cartridge.
const ReadAttrMediumTypeDataCartridge uint8 = 0

var ErrCanNotStatTheDevice error = errors.New("can not stat specified device file")
var ErrNoRewindRequired error = errors.New("a no rewind tape device is required")
var ErrUnsupportedPlatform error = errors.New("this os is not supported")
var ErrDeviceCheckFailed error = errors.New("device check failed")
var ErrNoMtDiscovered error = errors.New("can not find mt")
var ErrNoSgLogsDiscovered error = errors.New("can not find sg_logs")
var ErrNoSgReadAttrDiscovered error = errors.New("can not find sg_read_attr")
var ErrSgLogsExecFailed error = errors.New("failed to get sg_logs cmd output")
var ErrSgReadAttrExecFailed error = errors.New("failed to get sg_read_attr cmd output")
var ErrCanNotParseSgReadAttrOutput error = errors.New("can not parse output of sg_read_attr")
var ErrAttrParseFailed error = errors.New("failed to parse output of sg_read_attr")
var ErrSomeMtRelatedFieldsMissing error = errors.New("some mt related fields missing")
var ErrSomeSgRelatedFieldsMissing error = errors.New("some sg_* related fields missing")
var ErrSomeCapacityLogFieldsMissing error = errors.New("some capacity log fields missing")
var ErrCanNotParseMainPartitionRemaining error = errors.New("can not parse main partition remaining capacity")
var ErrCanNotParseAlternatePartitionRemaining error = errors.New("can not parse alternate partition remaining capacity")
var ErrCanNotParseMainPartitionMax error = errors.New("can not parse main partition maximum capacity")
var ErrCanNotParseAlternatePartitionMax error = errors.New("can not parse alternate partition maximum capacity")
var ErrNoDataCartridgeOrNotReady error = errors.New("no data cartridge or not ready yet")
var ErrCanNotRewind error = errors.New("can not rewind the tape")
var ErrMtExecFailed error = errors.New("failed to get mt cmd output")
var ErrMtOutputParseFailed error = errors.New("can not parse output of mt")
var ErrCanNotGoToPrevFile error = errors.New("can not go to previous file")
var ErrCanNotGetMediumSN error = errors.New("can not get medium serial number")

// It will detect related utils automatically, if some detection failed,
// you might need to set the utils binary manually.
func NewLtoNoRewindTapeDrive(device string) (LtoNoRewindTapeDrive, error) {
	newDevice := LtoNoRewindTapeDrive{
		DeviceFile: device,
		SgLogs:     "",
		Mt:         "",
		SgReadAttr: "",
	}
	var errs, err error
	if newDevice.tryStatDF() != nil {
		return newDevice, ErrCanNotStatTheDevice
	}
	if newDevice.chkIsNoRewind() != nil {
		return newDevice, ErrNoRewindRequired
	}
	if newDevice.Mt, err = discoverMt(); err != nil {
		errs = errors.Join(errs, err)
	}
	if newDevice.SgLogs, err = discoverSgLogs(); err != nil {
		errs = errors.Join(errs, err)
	}
	if newDevice.SgReadAttr, err = discoverSgReadAttr(); err != nil {
		errs = errors.Join(errs, err)
	}
	return newDevice, errs
}

func (device *LtoNoRewindTapeDrive) tryStatDF() error {
	_, err := os.Stat(device.DeviceFile)
	if err != nil {
		return ErrCanNotStatTheDevice
	}
	return nil
}

func (device *LtoNoRewindTapeDrive) chkIsNoRewind() error {
	splitedDF := strings.Split(device.DeviceFile, "/")
	switch runtime.GOOS {
	case "linux":
		if !strings.HasPrefix(splitedDF[len(splitedDF)-1], "nst") {
			return ErrNoRewindRequired
		}
	case "freebsd":
		fallthrough
	case "netbsd":
		fallthrough
	case "openbsd":
		if !strings.HasPrefix(splitedDF[len(splitedDF)-1], "nsa") {
			return ErrNoRewindRequired
		}
	case "solaris":
		if !strings.HasSuffix(splitedDF[len(splitedDF)-1], "n") {
			return ErrNoRewindRequired
		}
	case "aix":
		if !strings.HasSuffix(splitedDF[len(splitedDF)-1], ".1") {
			return ErrNoRewindRequired
		}
	default:
		return ErrUnsupportedPlatform
	}
	return nil
}

func (device *LtoNoRewindTapeDrive) ChkDevice() error {
	if _, err := device.ExecMtCmd(0, "status"); err != nil {
		return fmt.Errorf("%w: %w", ErrDeviceCheckFailed, err)
	}
	return nil
}

// Related attribute ID is 0408h.
func (device *LtoNoRewindTapeDrive) HasDataCartridge() (bool, error) {
	attrStr, err := device.ExecSgReadAttr("0x0408")
	if err != nil {
		return false, err
	}
	val, err := strconv.ParseInt(attrStr, 0, 64)
	if err != nil {
		return false, fmt.Errorf("%w: %w", ErrCanNotParseSgReadAttrOutput, err)
	}
	return val == int64(ReadAttrMediumTypeDataCartridge), nil
}

// Use IDs like 0x0408 or 0408h.
func (device *LtoNoRewindTapeDrive) ExecSgReadAttr(id string) (string, error) {
	cmdOut, err := getCmdOutput(device.SgReadAttr, "-f", id, device.DeviceFile)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrSgReadAttrExecFailed, err)
	}
	return extractSgStrVal(string(cmdOut))
}

func (device *LtoNoRewindTapeDrive) TryReadAttr() error {
	_, err := device.HasDataCartridge()
	return err
}

// From the Tape Capacity log page (code is 31h).
func (device *LtoNoRewindTapeDrive) GetCapacityLog() (LtoTapeCapacityLog, error) {
	var errs error
	capLog := LtoTapeCapacityLog{-1, -1, -1, -1}
	cmdOut, err := getCmdOutput(device.SgLogs, "-p", "0x31", device.DeviceFile)
	if err != nil {
		errs = errors.Join(errs, fmt.Errorf("%w: %w", ErrSgLogsExecFailed, err))
		return capLog, errs
	}
	outLines := strings.Split(string(cmdOut), "\n")
	for _, line := range outLines {
		tmp := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(tmp, "Main partition remaining capacity"):
			errs = errors.Join(errs, parseSgLogsCapacityLine(tmp, &capLog.MainPartitionRemaining, ErrCanNotParseMainPartitionRemaining))
		case strings.HasPrefix(tmp, "Alternate partition remaining capacity"):
			errs = errors.Join(errs, parseSgLogsCapacityLine(tmp, &capLog.AlternatePartitionRemaining, ErrCanNotParseAlternatePartitionRemaining))
		case strings.HasPrefix(tmp, "Main partition maximum capacity"):
			errs = errors.Join(errs, parseSgLogsCapacityLine(tmp, &capLog.MainPartitionMax, ErrCanNotParseMainPartitionMax))
		case strings.HasPrefix(tmp, "Alternate partition maximum capacity"):
			errs = errors.Join(errs, parseSgLogsCapacityLine(tmp, &capLog.AlternatePartitionMax, ErrCanNotParseAlternatePartitionMax))
		}
	}
	if capLog.AlternatePartitionMax == UnknownCapacity ||
		capLog.AlternatePartitionRemaining == UnknownCapacity ||
		capLog.MainPartitionMax == UnknownCapacity ||
		capLog.MainPartitionRemaining == UnknownCapacity {
		errs = errors.Join(errs, fmt.Errorf("%w: %w", ErrSomeCapacityLogFieldsMissing, ErrSomeSgRelatedFieldsMissing))
	}
	return capLog, errs
}

// From attribute ID 0401h.
func (device *LtoNoRewindTapeDrive) GetMediumSN() (string, error) {
	if err := device.TryReadAttr(); err != nil {
		return "", fmt.Errorf("%w: %w", ErrCanNotGetMediumSN, err)
	}
	sn, err := device.ExecSgReadAttr("0x0401")
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrCanNotGetMediumSN, err)
	}
	return sn, nil
}

func (device *LtoNoRewindTapeDrive) Rewind(timeout time.Duration) error {
	_, err := device.ExecMtCmd(timeout, "rewind")
	return err
}

func (device *LtoNoRewindTapeDrive) RewindCtx(ctx context.Context) error {
	_, err := device.ExecMtCmdCtx(ctx, "rewind")
	return err
}

func (device *LtoNoRewindTapeDrive) NextFile(timeout time.Duration) error {
	if timeout == 0 {
		return device.NextFileCtx(context.Background())
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return device.NextFileCtx(ctx)
}

func (device *LtoNoRewindTapeDrive) NextFileCtx(ctx context.Context) error {
	return device.FSFCtx(ctx, 1)
}

// This will cost a lot of time! Returned error will never be nil, it's the latest error.
//
// After get the count, it'll rewind the tape for you.
func (device *LtoNoRewindTapeDrive) CountFiles(timeout time.Duration) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var err error
	var cnt uint64 = 0
	err = device.RewindCtx(ctx)
	for err == nil {
		err = device.NextFileCtx(ctx)
		if err != nil {
			break
		}
		cnt++
	}
	errRewind := device.RewindCtx(ctx)
	return cnt, errors.Join(err, errRewind)
}

func (device *LtoNoRewindTapeDrive) PrevFile(timeout time.Duration) error {
	if timeout == 0 {
		return device.PrevFileCtx(context.Background())
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return device.PrevFileCtx(ctx)
}

func (device *LtoNoRewindTapeDrive) PrevFileCtx(ctx context.Context) error {
	curFileNo, err := device.GetCurFileNumber()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrCanNotGoToPrevFile, err)
	}
	if curFileNo == 0 || curFileNo == 1 {
		return device.RewindCtx(ctx)
	}
	err = device.BSFCtx(ctx, 1)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrCanNotGoToPrevFile, err)
	}
	err = device.BSFMCtx(ctx, 1)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrCanNotGoToPrevFile, err)
	}
	return nil
}

func (device *LtoNoRewindTapeDrive) FSF(count uint32, timeout time.Duration) error {
	_, err := device.ExecMtCmd(timeout, "fsf", count)
	return err
}

func (device *LtoNoRewindTapeDrive) FSFCtx(ctx context.Context, count uint32) error {
	_, err := device.ExecMtCmdCtx(ctx, "fsf", count)
	return err
}

func (device *LtoNoRewindTapeDrive) BSF(count uint32, timeout time.Duration) error {
	_, err := device.ExecMtCmd(timeout, "bsf", count)
	return err
}

func (device *LtoNoRewindTapeDrive) BSFCtx(ctx context.Context, count uint32) error {
	_, err := device.ExecMtCmdCtx(ctx, "bsf", count)
	return err
}

func (device *LtoNoRewindTapeDrive) BSFM(count uint32, timeout time.Duration) error {
	_, err := device.ExecMtCmd(timeout, "bsfm", count)
	return err
}

func (device *LtoNoRewindTapeDrive) BSFMCtx(ctx context.Context, count uint32) error {
	_, err := device.ExecMtCmdCtx(ctx, "bsfm", count)
	return err
}

func (device *LtoNoRewindTapeDrive) Erase(timeout time.Duration) error {
	_, err := device.ExecMtCmd(timeout, "erase")
	return err
}

func (device *LtoNoRewindTapeDrive) EraseCtx(ctx context.Context) error {
	_, err := device.ExecMtCmdCtx(ctx, "erase")
	return err
}

func (device *LtoNoRewindTapeDrive) Eject(timeout time.Duration) error {
	_, err := device.ExecMtCmd(timeout, "eject")
	return err
}

func (device *LtoNoRewindTapeDrive) EjectCtx(ctx context.Context) error {
	_, err := device.ExecMtCmdCtx(ctx, "eject")
	return err
}

func (device *LtoNoRewindTapeDrive) WEOF(timeout time.Duration) error {
	_, err := device.ExecMtCmd(timeout, "weof")
	return err
}

func (device *LtoNoRewindTapeDrive) WEOFCtx(ctx context.Context) error {
	_, err := device.ExecMtCmdCtx(ctx, "weof")
	return err
}

// This let you know which file you are using, NOT total files count!
func (device *LtoNoRewindTapeDrive) GetCurFileNumber() (uint64, error) {
	cmdOut, err := device.ExecMtCmd(0, "status")
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrMtExecFailed, err)
	}
	outLines := strings.Split(string(cmdOut), "\n")
	for _, line := range outLines {
		splited := strings.Split(line, ",")
		trimed := strings.TrimSpace(splited[0])
		if strings.HasPrefix(trimed, "File number") {
			val, err := extractMtUintVal(trimed, 0)
			if err != nil {
				return 0, fmt.Errorf("%w: %w", ErrMtOutputParseFailed, err)
			}
			return val, err
		}
	}
	return 0, ErrSomeMtRelatedFieldsMissing
}

func (device *LtoNoRewindTapeDrive) ExecMtCmd(timeout time.Duration, cmd string, args ...uint32) ([]byte, error) {
	if timeout == 0 {
		return device.ExecMtCmdCtx(context.Background(), cmd, args...)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return device.ExecMtCmdCtx(ctx, cmd, args...)
}

func (device *LtoNoRewindTapeDrive) ExecMtCmdCtx(ctx context.Context, cmd string, args ...uint32) ([]byte, error) {
	hasDC, err := device.HasDataCartridge()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrNoDataCartridgeOrNotReady, err)
	}
	if !hasDC {
		return nil, ErrNoDataCartridgeOrNotReady
	}
	argsStrs := make([]string, 3)
	argsStrs[0] = "-f"
	argsStrs[1] = device.DeviceFile
	argsStrs[2] = cmd
	for _, x := range args {
		argsStrs = append(argsStrs, strconv.FormatUint(uint64(x), 10))
	}
	return getCmdOutputCtx(ctx, device.Mt, argsStrs...)
}

func parseSgLogsCapacityLine(line string, target *int64, baseErr error) error {
	valInt, err := extractSgIntVal(line, 0)
	if err != nil {
		(*target) = UnknownCapacity
		return fmt.Errorf("%w: %w", baseErr, err)
	}
	(*target) = valInt
	return nil
}

func extractSgIntVal(s string, base int) (int64, error) {
	s = strings.TrimSpace(s)
	splited := strings.Split(s, ":")
	valStr := strings.TrimSpace(splited[len(splited)-1])
	if len(valStr) == 0 {
		return -1, ErrSomeSgRelatedFieldsMissing
	}
	valStr = strings.Fields(valStr)[0]
	valStr = strings.ReplaceAll(valStr, ",", "")
	valInt, err := strconv.ParseInt(valStr, base, 64)
	if err != nil {
		return -1, err
	}
	return valInt, nil
}

func extractSgStrVal(s string) (string, error) {
	s = strings.TrimSpace(s)
	splited := strings.Split(s, ":")
	if len(splited) < 2 {
		return "", ErrSomeSgRelatedFieldsMissing
	}
	valStr := strings.TrimSpace(splited[len(splited)-1])
	if len(valStr) == 0 || len(strings.Fields(valStr)[0]) == 0 {
		return "", ErrSomeSgRelatedFieldsMissing
	}
	return strings.Fields(valStr)[0], nil
}

func extractMtUintVal(s string, base int) (uint64, error) {
	s = strings.TrimSpace(s)
	splited := strings.Split(s, "=")
	valStr := strings.TrimSpace(splited[len(splited)-1])
	if len(valStr) == 0 {
		return 0, ErrSomeMtRelatedFieldsMissing
	}
	valStr = strings.Fields(valStr)[0]
	valStr = strings.ReplaceAll(valStr, ",", "")
	valInt, err := strconv.ParseUint(valStr, base, 64)
	if err != nil {
		return 0, err
	}
	return valInt, nil
}

func getCmdOutput(name string, args ...string) ([]byte, error) {
	cmd := exec.Command(name, args...)
	if EnableLtoUtilsWrapperDebugMode {
		fmt.Println("Lto Utils Wrapper: Run: ", name, args)
	}
	output, err := cmd.CombinedOutput()
	if EnableLtoUtilsWrapperDebugMode {
		fmt.Println("Output:")
		fmt.Println(string(output))
	}
	return output, err
}

func getCmdOutputCtx(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	if EnableLtoUtilsWrapperDebugMode {
		fmt.Println("Lto Utils Wrapper: Run: ", name, args)
	}
	output, err := cmd.CombinedOutput()
	if EnableLtoUtilsWrapperDebugMode {
		fmt.Println("Output:")
		fmt.Println(string(output))
	}
	return output, err
}

var possibleSgLogs = []string{"/usr/bin/sg_logs", "/bin/sg_logs", "sg_logs"}
var possibleSgReadAttr = []string{"/usr/bin/sg_read_attr", "/bin/sg_read_attr", "sg_read_attr"}
var possibleMt = []string{"/usr/bin/mt", "/usr/bin/mt-st", "/bin/mt", "/bin/mt-st", "mt", "mt-st"}

const UtilsDiscoverTimeout time.Duration = 1 * time.Second

func discoverSgLogs() (string, error) {
	return utilProbe(possibleSgLogs, "-h", ErrNoSgLogsDiscovered)
}

func discoverMt() (string, error) {
	return utilProbe(possibleMt, "-h", ErrNoMtDiscovered)
}

func discoverSgReadAttr() (string, error) {
	return utilProbe(possibleSgReadAttr, "-h", ErrNoSgReadAttrDiscovered)
}

func utilProbe(possibleAt []string, parm string, onFail error) (string, error) {
	var exitErr *exec.ExitError
	ctx, cancel := context.WithTimeout(context.Background(), UtilsDiscoverTimeout)
	defer cancel()
	for _, path := range possibleAt {
		if _, err := getCmdOutputCtx(ctx, path, parm); err == nil || errors.As(err, &exitErr) {
			return path, nil
		}
	}
	return "", onFail
}
