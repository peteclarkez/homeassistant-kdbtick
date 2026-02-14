"""
Python port of the KX Systems Java KDB+ IPC client (c.java).

Original source:
  https://github.com/KxSystems/javakdb/blob/master/javakdb/src/main/java/com/kx/c.java

Copyright (c) 1998-2017 Kx Systems Inc.
Licensed under the Apache License, Version 2.0.

This is a faithful translation of the Java reference implementation to pure Python,
using only stdlib modules (socket, struct, uuid, datetime). No external dependencies.
"""

import socket
import struct
import sys
import uuid
from datetime import date, datetime, time, timedelta, timezone
from io import BytesIO

ENCODING = "ISO-8859-1"


class c:
    """KDB+ IPC client. Python port of com.kx.c from javakdb."""

    # --- Null sentinel values ---
    ni = -(2**31)          # int null
    nj = -(2**63)          # long null
    nf = float("nan")      # float null

    # Element sizes indexed by kdb+ type code: nt[abs(type)]
    _nt = [0, 1, 16, 0, 1, 2, 4, 8, 4, 8, 1, 0, 8, 4, 4, 8, 8, 4, 4, 4]

    DAYS_BETWEEN_1970_2000 = 10957
    MILLIS_IN_DAY = 86400000
    MILLIS_BETWEEN_1970_2000 = MILLIS_IN_DAY * DAYS_BETWEEN_1970_2000
    NANOS_IN_SEC = 1000000000

    # ------------------------------------------------------------------
    # Inner classes (matching Java)
    # ------------------------------------------------------------------

    class KException(Exception):
        """KDB+ server error."""
        pass

    class Month:
        """KDB+ month type."""
        def __init__(self, i):
            self.i = i

        def __repr__(self):
            if self.i == c.ni:
                return ""
            m = self.i + 24000
            y = m // 12
            return f"{y:04d}-{1 + m % 12:02d}"

        def __eq__(self, other):
            return isinstance(other, c.Month) and self.i == other.i

        def __hash__(self):
            return self.i

    class Minute:
        """KDB+ minute type."""
        def __init__(self, i):
            self.i = i

        def __repr__(self):
            if self.i == c.ni:
                return ""
            return f"{self.i // 60:02d}:{self.i % 60:02d}"

        def __eq__(self, other):
            return isinstance(other, c.Minute) and self.i == other.i

        def __hash__(self):
            return self.i

    class Second:
        """KDB+ second type."""
        def __init__(self, i):
            self.i = i

        def __repr__(self):
            if self.i == c.ni:
                return ""
            return f"{c.Minute(self.i // 60)}:{self.i % 60:02d}"

        def __eq__(self, other):
            return isinstance(other, c.Second) and self.i == other.i

        def __hash__(self):
            return self.i

    class Timespan:
        """KDB+ timespan type (nanoseconds)."""
        def __init__(self, j):
            self.j = j

        def __repr__(self):
            if self.j == c.nj:
                return ""
            s = "-" if self.j < 0 else ""
            jj = abs(self.j)
            d = int(jj // 86400000000000)
            if d != 0:
                s += f"{d}D"
            h = int((jj % 86400000000000) // 3600000000000)
            m = int((jj % 3600000000000) // 60000000000)
            sec = int((jj % 60000000000) // 1000000000)
            ns = int(jj % 1000000000)
            return f"{s}{h:02d}:{m:02d}:{sec:02d}.{ns:09d}"

        def __eq__(self, other):
            return isinstance(other, c.Timespan) and self.j == other.j

        def __hash__(self):
            return hash(self.j)

    class Dict:
        """KDB+ dictionary: maps keys (x) to values (y)."""
        def __init__(self, x, y):
            self.x = x
            self.y = y

    class Flip:
        """KDB+ table: column names (x) and column data (y)."""
        def __init__(self, x_or_dict, y=None):
            if isinstance(x_or_dict, c.Dict):
                self.x = x_or_dict.x
                self.y = x_or_dict.y
            else:
                self.x = x_or_dict
                self.y = y

        def at(self, s):
            """Get column data by name."""
            idx = list(self.x).index(s)
            return self.y[idx]

    class CharVector:
        """Wrapper to explicitly send a Python string as a kdb+ char vector (type 10)
        rather than a symbol (type -11). Used internally by k()/ks() for function names."""
        def __init__(self, s):
            if isinstance(s, str):
                self.data = s.encode(ENCODING)
            else:
                self.data = bytes(s)

        def __len__(self):
            return len(self.data)

    # ------------------------------------------------------------------
    # Construction / Connection
    # ------------------------------------------------------------------

    def __init__(self, host=None, port=None, usernamepassword=None, use_tls=False, timeout=0):
        """Connect to a kdb+ server.

        Args:
            host: Hostname or IP. If None, creates a disconnected instance.
            port: Port number.
            usernamepassword: "user:pass" string. Defaults to empty.
            use_tls: Enable TLS (requires ssl module).
            timeout: Socket timeout in seconds (0 = no timeout).
        """
        self._sync = 0
        self.s = None
        self.ipc_version = 3
        self._is_little_endian = True  # for reading; set from server response
        self._is_loopback = False
        self._zip = False

        # Read/write buffers
        self._r_buf = bytearray()
        self._r_pos = 0
        self._w_buf = bytearray()
        self._w_pos = 0

        if host is None:
            return

        if usernamepassword is None:
            usernamepassword = ""

        # Connect
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if timeout > 0:
            self.s.settimeout(timeout)
        self.s.connect((host, port))
        self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

        if use_tls:
            import ssl
            ctx = ssl.create_default_context()
            self.s = ctx.wrap_socket(self.s, server_hostname=host)

        addr = self.s.getpeername()[0]
        self._is_loopback = addr in ("127.0.0.1", "::1", "0.0.0.0")

        # Handshake
        cred_bytes = (usernamepassword + "\x03").encode(ENCODING) + b"\x00"
        self.s.sendall(cred_bytes)
        resp = self.s.recv(1)
        if len(resp) != 1:
            self.close()
            raise c.KException("access")
        self.ipc_version = min(resp[0], 3)

    def close(self):
        """Close the connection."""
        if self.s is not None:
            try:
                self.s.close()
            except Exception:
                pass
            self.s = None

    def zip(self, b):
        """Enable/disable IPC compression."""
        self._zip = b

    # ------------------------------------------------------------------
    # Read primitives (endian-aware, matching Java)
    # ------------------------------------------------------------------

    def _rb(self):
        """Read boolean."""
        val = self._r_buf[self._r_pos]
        self._r_pos += 1
        return val == 1

    def _rc(self):
        """Read char."""
        val = self._r_buf[self._r_pos] & 0xFF
        self._r_pos += 1
        return chr(val)

    def _rh(self):
        """Read short (2 bytes)."""
        x = self._r_buf[self._r_pos]
        y = self._r_buf[self._r_pos + 1]
        self._r_pos += 2
        if self._is_little_endian:
            return struct.unpack("<h", bytes([x, y]))[0]
        else:
            return struct.unpack(">h", bytes([x, y]))[0]

    def _ri(self):
        """Read int (4 bytes)."""
        x = self._rh() & 0xFFFF
        y = self._rh() & 0xFFFF
        if self._is_little_endian:
            return (x & 0xFFFF) | (y << 16)
        else:
            return (x << 16) | (y & 0xFFFF)

    def _rj(self):
        """Read long (8 bytes)."""
        x = self._ri() & 0xFFFFFFFF
        y = self._ri() & 0xFFFFFFFF
        if self._is_little_endian:
            val = (x & 0xFFFFFFFF) | (y << 32)
        else:
            val = (x << 32) | (y & 0xFFFFFFFF)
        # Convert to signed 64-bit
        if val >= 2**63:
            val -= 2**64
        return val

    def _re(self):
        """Read float (4 bytes)."""
        val = self._ri()
        return struct.unpack("=f", struct.pack("=I", val & 0xFFFFFFFF))[0]

    def _rf(self):
        """Read double (8 bytes)."""
        val = self._rj()
        return struct.unpack("=d", struct.pack("=Q", val & 0xFFFFFFFFFFFFFFFF))[0]

    def _rg(self):
        """Read UUID (16 bytes)."""
        old = self._is_little_endian
        self._is_little_endian = False
        msb = self._rj()
        lsb = self._rj()
        self._is_little_endian = old
        # Convert signed longs back to unsigned for UUID construction
        if msb < 0:
            msb += 2**64
        if lsb < 0:
            lsb += 2**64
        return uuid.UUID(int=(msb << 64) | lsb)

    def _rs(self):
        """Read null-terminated symbol string."""
        start = self._r_pos
        while self._r_buf[self._r_pos] != 0:
            self._r_pos += 1
        s = self._r_buf[start:self._r_pos].decode(ENCODING) if self._r_pos > start else ""
        self._r_pos += 1  # skip null terminator
        return s

    def _rd(self):
        """Read date → datetime.date."""
        d = self._ri()
        if d == c.ni:
            return date.min
        return date.fromordinal(date(2000, 1, 1).toordinal() + d)

    def _rt(self):
        """Read time → datetime.time."""
        t_val = self._ri()
        if t_val == c.ni:
            return time(0, 0, 0, 1)  # null sentinel
        ms = t_val % c.MILLIS_IN_DAY
        h = ms // 3600000
        m = (ms % 3600000) // 60000
        s = (ms % 60000) // 1000
        us = (ms % 1000) * 1000
        return time(h, m, s, us)

    def _rz(self):
        """Read datetime (float days from 2000.01.01) → datetime.datetime."""
        f = self._rf()
        if f != f:  # NaN check
            return datetime.min
        ms = c.MILLIS_BETWEEN_1970_2000 + round(8.64e7 * f)
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).replace(tzinfo=None)

    def _rp(self):
        """Read timestamp (nanos from 2000.01.01) → datetime.datetime."""
        j = self._rj()
        if j == c.nj:
            return datetime.min
        # Convert nanos since 2000.01.01 to epoch millis + sub-millis
        d = (j - 1) // c.NANOS_IN_SEC - 1 if j < 0 else j // c.NANOS_IN_SEC
        epoch_ms = c.MILLIS_BETWEEN_1970_2000 + 1000 * d
        sub_ns = j - c.NANOS_IN_SEC * d
        us = sub_ns // 1000  # microseconds component
        return datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc).replace(tzinfo=None) + timedelta(microseconds=us % 1000000)

    def _rn(self):
        """Read timespan."""
        return c.Timespan(self._rj())

    def _rm(self):
        """Read month."""
        return c.Month(self._ri())

    def _ru(self):
        """Read minute."""
        return c.Minute(self._ri())

    def _rv(self):
        """Read second."""
        return c.Second(self._ri())

    # ------------------------------------------------------------------
    # Write primitives (big-endian, matching Java)
    # ------------------------------------------------------------------

    def _wb(self, x):
        """Write a single byte."""
        self._w_buf[self._w_pos] = x & 0xFF
        self._w_pos += 1

    def _w_bool(self, x):
        """Write boolean."""
        self._wb(1 if x else 0)

    def _wc(self, ch):
        """Write char."""
        self._wb(ord(ch) if isinstance(ch, str) else ch)

    def _wh(self, h):
        """Write short (2 bytes, big-endian)."""
        h = h & 0xFFFF
        self._wb((h >> 8) & 0xFF)
        self._wb(h & 0xFF)

    def _wi(self, i):
        """Write int (4 bytes, big-endian)."""
        i = i & 0xFFFFFFFF
        self._wh((i >> 16) & 0xFFFF)
        self._wh(i & 0xFFFF)

    def _wj(self, j):
        """Write long (8 bytes, big-endian)."""
        j = j & 0xFFFFFFFFFFFFFFFF
        self._wi((j >> 32) & 0xFFFFFFFF)
        self._wi(j & 0xFFFFFFFF)

    def _we(self, e):
        """Write float (4 bytes)."""
        self._wi(struct.unpack("=I", struct.pack("=f", e))[0])

    def _wf(self, f):
        """Write double (8 bytes)."""
        self._wj(struct.unpack("=Q", struct.pack("=d", f))[0])

    def _wg(self, g):
        """Write UUID (16 bytes)."""
        if self.ipc_version < 3:
            raise RuntimeError("Guid not valid pre kdb+3.0")
        val = g.int
        self._wj((val >> 64) & 0xFFFFFFFFFFFFFFFF)
        self._wj(val & 0xFFFFFFFFFFFFFFFF)

    def _ws(self, s):
        """Write null-terminated symbol string."""
        if s is not None:
            encoded = s.encode(ENCODING)
            for b in encoded:
                self._wb(b)
        self._wb(0)

    def _wd(self, d):
        """Write date."""
        if d == date.min:
            self._wi(c.ni)
        else:
            days = d.toordinal() - date(2000, 1, 1).toordinal()
            self._wi(days)

    def _wt(self, t):
        """Write time."""
        if t == time(0, 0, 0, 1):  # null sentinel
            self._wi(c.ni)
        else:
            ms = (t.hour * 3600 + t.minute * 60 + t.second) * 1000 + t.microsecond // 1000
            self._wi(ms)

    def _wz(self, z):
        """Write datetime."""
        if z == datetime.min:
            self._wf(c.nf)
        else:
            epoch_ms = z.replace(tzinfo=timezone.utc).timestamp() * 1000
            self._wf((epoch_ms - c.MILLIS_BETWEEN_1970_2000) / 8.64e7)

    def _wp(self, p):
        """Write timestamp."""
        if self.ipc_version < 1:
            raise RuntimeError("Timestamp not valid pre kdb+2.6")
        if p == datetime.min:
            self._wj(c.nj)
        else:
            epoch_ms = p.replace(tzinfo=timezone.utc).timestamp() * 1000
            us = p.microsecond
            self._wj(int(1000000 * (epoch_ms - c.MILLIS_BETWEEN_1970_2000) + us % 1000000))

    def _wn(self, n):
        """Write timespan."""
        if self.ipc_version < 1:
            raise RuntimeError("Timespan not valid pre kdb+2.6")
        self._wj(n.j)

    def _wm(self, m):
        """Write month."""
        self._wi(m.i)

    def _wu(self, u):
        """Write minute."""
        self._wi(u.i)

    def _wv(self, v):
        """Write second."""
        self._wi(v.i)

    # ------------------------------------------------------------------
    # Type detection (matching Java's t() method)
    # ------------------------------------------------------------------

    @staticmethod
    def t(x):
        """Return the kdb+ type code for a Python object."""
        if isinstance(x, bool):
            return -1
        if isinstance(x, uuid.UUID):
            return -2
        if isinstance(x, int) and not isinstance(x, bool):
            return -7
        if isinstance(x, float):
            return -9
        if isinstance(x, str):
            return -11
        if isinstance(x, c.CharVector):
            return 10
        if isinstance(x, date) and not isinstance(x, datetime):
            return -14
        if isinstance(x, time) and not isinstance(x, datetime):
            return -19
        if isinstance(x, datetime):
            return -15
        if isinstance(x, c.Timespan):
            return -16
        if isinstance(x, c.Month):
            return -13
        if isinstance(x, c.Minute):
            return -17
        if isinstance(x, c.Second):
            return -18
        if isinstance(x, c.Flip):
            return 98
        if isinstance(x, c.Dict):
            return 99
        if isinstance(x, (list, tuple)):
            return 0
        if isinstance(x, bytearray):
            return 4
        if isinstance(x, bytes):
            return 4
        return 0

    # ------------------------------------------------------------------
    # Null handling
    # ------------------------------------------------------------------

    @staticmethod
    def NULL(ch):
        """Return the null value for a given kdb+ type char."""
        idx = " bg xhijefcspmdznuvt".index(ch)
        nulls = [
            None,                           # 0: general
            False,                          # 1: boolean
            uuid.UUID(int=0),               # 2: guid
            None,                           # 3: (unused)
            0,                              # 4: byte
            -(2**15),                       # 5: short
            c.ni,                           # 6: int
            c.nj,                           # 7: long
            float("nan"),                   # 8: real
            float("nan"),                   # 9: float
            " ",                            # 10: char
            "",                             # 11: symbol
            datetime.min,                   # 12: timestamp
            c.Month(c.ni),                  # 13: month
            date.min,                       # 14: date
            datetime.min,                   # 15: datetime
            c.Timespan(c.nj),               # 16: timespan
            c.Minute(c.ni),                 # 17: minute
            c.Second(c.ni),                 # 18: second
            time(0, 0, 0, 1),              # 19: time
        ]
        return nulls[idx]

    # ------------------------------------------------------------------
    # Size calculation (matching Java's nx())
    # ------------------------------------------------------------------

    def nx(self, x):
        """Calculate the serialized byte size of object x."""
        t = c.t(x)
        if t == 99:
            return 1 + self.nx(x.x) + self.nx(x.y)
        if t == 98:
            return 3 + self.nx(x.x) + self.nx(x.y)
        if t < 0:
            if t == -11:
                return 2 + len(x.encode(ENCODING))
            return 1 + c._nt[-t]

        # Vectors and lists
        n_bytes = 6
        n_elems = self._n(x)
        if t == 0:
            for item in x:
                n_bytes += self.nx(item)
        elif t == 10:
            n_bytes += len(x)
        elif t == 11:
            for item in x:
                n_bytes += 1 + len(item.encode(ENCODING))
        else:
            n_bytes += n_elems * c._nt[t]
        return n_bytes

    @staticmethod
    def _n(x):
        """Return the number of elements in a vector/list."""
        if isinstance(x, c.Dict):
            return c._n(x.x)
        if isinstance(x, c.Flip):
            return c._n(x.y[0])
        if isinstance(x, c.CharVector):
            return len(x.data)
        return len(x)

    # ------------------------------------------------------------------
    # Object reader (matching Java's r())
    # ------------------------------------------------------------------

    def r(self):
        """Read and deserialize one object from the read buffer."""
        t = struct.unpack("b", bytes([self._r_buf[self._r_pos]]))[0]
        self._r_pos += 1

        if t < 0:
            if t == -1:
                return self._rb()
            if t == -2:
                return self._rg()
            if t == -4:
                val = self._r_buf[self._r_pos]
                self._r_pos += 1
                return val
            if t == -5:
                return self._rh()
            if t == -6:
                return self._ri()
            if t == -7:
                return self._rj()
            if t == -8:
                return self._re()
            if t == -9:
                return self._rf()
            if t == -10:
                return self._rc()
            if t == -11:
                return self._rs()
            if t == -12:
                return self._rp()
            if t == -13:
                return self._rm()
            if t == -14:
                return self._rd()
            if t == -15:
                return self._rz()
            if t == -16:
                return self._rn()
            if t == -17:
                return self._ru()
            if t == -18:
                return self._rv()
            if t == -19:
                return self._rt()

        if t > 99:
            if t == 100:
                self._rs()
                return self.r()
            if t < 104:
                val = self._r_buf[self._r_pos]
                self._r_pos += 1
                if val == 0 and t == 101:
                    return None
                return "func"
            if t > 105:
                self.r()
            else:
                n = self._ri()
                for _ in range(n):
                    self.r()
            return "func"

        if t == 99:
            return c.Dict(self.r(), self.r())

        # Skip attribute byte
        self._r_pos += 1

        if t == 98:
            return c.Flip(self.r())

        n = self._ri()

        if t == 0:
            return [self.r() for _ in range(n)]
        if t == 1:
            return [self._rb() for _ in range(n)]
        if t == 2:
            return [self._rg() for _ in range(n)]
        if t == 4:
            result = bytearray(self._r_buf[self._r_pos:self._r_pos + n])
            self._r_pos += n
            return result
        if t == 5:
            return [self._rh() for _ in range(n)]
        if t == 6:
            return [self._ri() for _ in range(n)]
        if t == 7:
            return [self._rj() for _ in range(n)]
        if t == 8:
            return [self._re() for _ in range(n)]
        if t == 9:
            return [self._rf() for _ in range(n)]
        if t == 10:
            chars = self._r_buf[self._r_pos:self._r_pos + n].decode(ENCODING)
            self._r_pos += n
            return chars
        if t == 11:
            return [self._rs() for _ in range(n)]
        if t == 12:
            return [self._rp() for _ in range(n)]
        if t == 13:
            return [self._rm() for _ in range(n)]
        if t == 14:
            return [self._rd() for _ in range(n)]
        if t == 15:
            return [self._rz() for _ in range(n)]
        if t == 16:
            return [self._rn() for _ in range(n)]
        if t == 17:
            return [self._ru() for _ in range(n)]
        if t == 18:
            return [self._rv() for _ in range(n)]
        if t == 19:
            return [self._rt() for _ in range(n)]

        return None

    # ------------------------------------------------------------------
    # Object writer (matching Java's w(Object))
    # ------------------------------------------------------------------

    def w(self, x):
        """Serialize one object into the write buffer."""
        t = c.t(x)
        self._wb(t & 0xFF)

        if t < 0:
            if t == -1:
                self._w_bool(x)
            elif t == -2:
                self._wg(x)
            elif t == -4:
                self._wb(x)
            elif t == -5:
                self._wh(x)
            elif t == -6:
                self._wi(x)
            elif t == -7:
                self._wj(x)
            elif t == -8:
                self._we(x)
            elif t == -9:
                self._wf(x)
            elif t == -10:
                self._wc(x)
            elif t == -11:
                self._ws(x)
            elif t == -12:
                self._wp(x)
            elif t == -13:
                self._wm(x)
            elif t == -14:
                self._wd(x)
            elif t == -15:
                self._wz(x)
            elif t == -16:
                self._wn(x)
            elif t == -17:
                self._wu(x)
            elif t == -18:
                self._wv(x)
            elif t == -19:
                self._wt(x)
            return

        if t == 99:
            self.w(x.x)
            self.w(x.y)
            return

        # Attribute byte
        self._wb(0)

        if t == 98:
            self._wb(99)
            self.w(x.x)
            self.w(x.y)
            return

        n = self._n(x)
        self._wi(n)

        if t == 10:
            # CharVector
            data = x.data if isinstance(x, c.CharVector) else x.encode(ENCODING)
            for b in data:
                self._wb(b)
        elif t == 0:
            for item in x:
                self.w(item)
        elif t == 11:
            for item in x:
                self._ws(item)
        elif t == 1:
            for item in x:
                self._w_bool(item)
        elif t == 2:
            for item in x:
                self._wg(item)
        elif t == 4:
            for item in x:
                self._wb(item)
        elif t == 5:
            for item in x:
                self._wh(item)
        elif t == 6:
            for item in x:
                self._wi(item)
        elif t == 7:
            for item in x:
                self._wj(item)
        elif t == 8:
            for item in x:
                self._we(item)
        elif t == 9:
            for item in x:
                self._wf(item)
        elif t == 12:
            for item in x:
                self._wp(item)
        elif t == 13:
            for item in x:
                self._wm(item)
        elif t == 14:
            for item in x:
                self._wd(item)
        elif t == 15:
            for item in x:
                self._wz(item)
        elif t == 16:
            for item in x:
                self._wn(item)
        elif t == 17:
            for item in x:
                self._wu(item)
        elif t == 18:
            for item in x:
                self._wv(item)
        elif t == 19:
            for item in x:
                self._wt(item)

    # ------------------------------------------------------------------
    # Compression / Decompression (matching Java)
    # ------------------------------------------------------------------

    def _compress(self):
        """Compress the write buffer using kdb+'s IPC compression."""
        i = 0
        orig_size = self._w_pos
        f = 0
        h0 = 0
        h = 0
        y = self._w_buf
        self._w_buf = bytearray(len(y) // 2)
        cc = 12
        d = cc
        e = len(self._w_buf)
        p = 0
        s0 = 0
        s = 8
        t = self._w_pos
        a = [0] * 256
        self._w_buf[:4] = y[:4]
        self._w_buf[2] = 1
        self._w_pos = 8
        self._wi(orig_size)
        while s < t:
            if i == 0:
                if d > e - 17:
                    self._w_pos = orig_size
                    self._w_buf = y
                    return
                i = 1
                self._w_buf[cc] = f & 0xFF
                cc = d
                d += 1
                f = 0

            g = (s > t - 3) or (0 == (p := a[h := 0xFF & (y[s] ^ y[s + 1])])) or (0 != (y[s] ^ y[p]))
            if s0 > 0:
                a[h0] = s0
                s0 = 0
            if g:
                h0 = h
                s0 = s
                self._w_buf[d] = y[s]
                d += 1
                s += 1
            else:
                a[h] = s
                f |= i
                p += 2
                r = s
                s += 2
                q = min(s + 255, t)
                while s < q and y[p] == y[s]:
                    p += 1
                    s += 1
                self._w_buf[d] = h & 0xFF
                d += 1
                self._w_buf[d] = (s - r) & 0xFF
                d += 1

            i *= 2
            if i == 256:
                i = 0

        self._w_buf[cc] = f & 0xFF
        self._w_pos = 4
        self._wi(d)
        self._w_pos = d
        self._w_buf = bytearray(self._w_buf[:self._w_pos])

    def _uncompress(self):
        """Decompress a compressed IPC message."""
        n = 0
        r = 0
        f = 0
        s = 8
        p = s
        i = 0
        dst_size = self._ri()
        dst = bytearray(dst_size)
        d = self._r_pos
        aa = [0] * 256
        while s < len(dst):
            if i == 0:
                f = self._r_buf[d] & 0xFF
                d += 1
                i = 1
            if (f & i) != 0:
                r = aa[self._r_buf[d] & 0xFF]
                d += 1
                dst[s] = dst[r]
                s += 1
                r += 1
                dst[s] = dst[r]
                s += 1
                r += 1
                n = self._r_buf[d] & 0xFF
                d += 1
                for m in range(n):
                    dst[s + m] = dst[r + m]
            else:
                dst[s] = self._r_buf[d]
                d += 1
                s += 1
            while p < s - 1:
                aa[(dst[p] & 0xFF) ^ (dst[p + 1] & 0xFF)] = p
                p += 1
            if (f & i) != 0:
                s += n
                p = s
            i *= 2
            if i == 256:
                i = 0
        self._r_buf = dst
        self._r_pos = 8

    # ------------------------------------------------------------------
    # Serialization / Deserialization
    # ------------------------------------------------------------------

    def serialize(self, msg_type, x, do_zip=False):
        """Serialize object x into an IPC message buffer."""
        length = 8 + self.nx(x)
        self._w_buf = bytearray(length)
        self._w_buf[0] = 0  # big-endian (matching Java)
        self._w_buf[1] = msg_type
        self._w_pos = 4
        self._wi(length)
        self.w(x)
        if do_zip and self._w_pos > 2000 and not self._is_loopback:
            self._compress()
        return bytes(self._w_buf[:self._w_pos])

    def deserialize(self, buffer):
        """Deserialize an IPC message buffer into a Python object."""
        self._r_buf = buffer if isinstance(buffer, (bytearray, memoryview)) else bytearray(buffer)
        self._is_little_endian = self._r_buf[0] == 1
        compressed = self._r_buf[2] == 1
        self._r_pos = 8
        if compressed:
            self._uncompress()
        if self._r_buf[8] == 0x80:  # -128 signed = error
            self._r_pos = 9
            raise c.KException(self._rs())
        return self.r()

    # ------------------------------------------------------------------
    # Low-level I/O
    # ------------------------------------------------------------------

    def _recv_all(self, n):
        """Read exactly n bytes from the socket."""
        buf = bytearray()
        while len(buf) < n:
            chunk = self.s.recv(n - len(buf))
            if not chunk:
                raise IOError("Connection closed")
            buf.extend(chunk)
        return buf

    def _send(self, buf):
        """Send bytes to the socket."""
        self.s.sendall(buf)

    def _w_msg(self, msg_type, x):
        """Serialize and send a message."""
        buf = self.serialize(msg_type, x, self._zip)
        self._send(buf)

    # ------------------------------------------------------------------
    # Message reading
    # ------------------------------------------------------------------

    def read_msg(self):
        """Read one IPC message. Returns (msg_type, deserialized_object)."""
        header = self._recv_all(8)
        self._r_buf = bytearray(header)
        self._is_little_endian = header[0] == 1
        msg_type = header[1]
        if msg_type == 1:
            self._sync += 1
        self._r_pos = 4
        msg_size = self._ri()
        # Read rest of message
        body = self._recv_all(msg_size - 8)
        self._r_buf = bytearray(header) + body
        return (msg_type, self.deserialize(self._r_buf))

    # ------------------------------------------------------------------
    # Server response helpers
    # ------------------------------------------------------------------

    def kr(self, obj):
        """Send a response message (type 2)."""
        if self._sync == 0:
            raise IOError("Unexpected response msg")
        self._sync -= 1
        self._w_msg(2, obj)

    def ke(self, text):
        """Send an error response."""
        if self._sync == 0:
            raise IOError("Unexpected error msg")
        self._sync -= 1
        n = 2 + len(text.encode(ENCODING)) + 8
        self._w_buf = bytearray(n)
        self._w_buf[0] = 0
        self._w_buf[1] = 2
        self._w_pos = 4
        self._wi(n)
        self._wb(0x80)  # -128 signed = error type
        self._ws(text)
        self._send(bytes(self._w_buf[:self._w_pos]))

    # ------------------------------------------------------------------
    # Public API: async send (matching Java's ks())
    # ------------------------------------------------------------------

    def ks(self, *args):
        """Send an async message.

        ks(expr)           — evaluate expression
        ks(func, x)        — call function with 1 arg
        ks(func, x, y)     — call function with 2 args
        ks(func, x, y, z)  — call function with 3 args
        """
        if len(args) == 1:
            x = args[0]
            if isinstance(x, str):
                self._w_msg(0, c.CharVector(x))
            else:
                self._w_msg(0, x)
        else:
            func = args[0]
            a = [c.CharVector(func) if isinstance(func, str) else func] + list(args[1:])
            self._w_msg(0, a)

    # ------------------------------------------------------------------
    # Public API: sync query/call (matching Java's k())
    # ------------------------------------------------------------------

    def k(self, *args):
        """Send a sync message and return the response.

        k()                — read next message (no send)
        k(expr)            — evaluate expression, return result
        k(func, x)         — call function with 1 arg
        k(func, x, y)      — call function with 2 args
        k(func, x, y, z)   — call function with 3 args
        """
        if len(args) == 0:
            return self.read_msg()[1]

        if len(args) == 1:
            x = args[0]
            if isinstance(x, str):
                obj = c.CharVector(x)
            else:
                obj = x
        else:
            func = args[0]
            a = [c.CharVector(func) if isinstance(func, str) else func] + list(args[1:])
            obj = a

        self._w_msg(1, obj)

        # Read response, handling any async messages that arrive first
        while True:
            msg = self.read_msg()
            if msg[0] == 2:  # response
                return msg[1]
            # Async message arrived while waiting - ignore it

    # ------------------------------------------------------------------
    # Utility: table helper (matching Java's td())
    # ------------------------------------------------------------------

    @staticmethod
    def td(tbl):
        """Unkey a keyed table."""
        if isinstance(tbl, c.Flip):
            return tbl
        d = tbl
        a = d.x
        b = d.y
        m = len(a.x)
        n_val = len(b.x)
        x = list(a.x) + list(b.x)
        y = list(a.y) + list(b.y)
        return c.Flip(c.Dict(x, y))
