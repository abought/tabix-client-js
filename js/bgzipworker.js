//This is a worker so we can use the synchronous filesystem API
importScripts('rawinflate.js');

self.requestFileSystemSync = self.webkitRequestFileSystemSync || self.requestFileSystemSync;

let bgzipdatafile;
let tabixfile;

self.addEventListener('message', function (e) {
    let data = e.data;
    let cmd = data.cmd || "";

    if (cmd === "setfiles") {
        setFiles(data.files);
    } else if (cmd === "getrange") {
        getDataRange(data.range);
    } else if (cmd === "peek") {
        getDataPeek(data.peek);
    } else {
        postMessage("hello");
    }
}, false);

function setFiles(files) {
    for (let i = 0; i < files.length; i++) {
        let bgz = new BGZipFile(files[i]);
        let bgzheader = bgz.getGzipHeader();
        if (bgzheader && bgzheader.bgzip) {
            let bgzreader = getBGZipReader(bgz);
            let tbiheader = readTabixDataHeader(bgzreader);
            if (tbiheader) {
                tabixfile = files[i];
            } else {
                bgzipdatafile = files[i];
            }
        }
    }
    if (bgzipdatafile && tabixfile) {
        postMessage({msg: "filesset"});
    } else {
        postMessage({msg: "filesnotset", ff: [bgzipdatafile, tabixfile]});
    }
}

function getDataPeek(n) {
    n = n || 25;
    let tbi = new BGZipFile(tabixfile);
    let tdr = getBGZipReader(tbi);
    let tbiheader = readTabixDataHeader(tdr);
    let bgz = new BGZipFile(bgzipdatafile);
    tdr = getBGZipReader(bgz);
    let a = "";
    for (let i = 0; i < n; i++) {
        a += tdr.readColumns().join(",") + "\n";
    }
    postMessage({box: a});
}

function getDataRange(rawrange) {
    let range = parseRange(rawrange);
    let tbi = new BGZipFile(tabixfile);
    let tdr = getBGZipReader(tbi);
    let tbiheader = readTabixDataHeader(tdr);
    let offset = findTabixOffset(tdr, tbiheader, range[0], range[1]);
    tdr = null; //done with this
    //postMessage( tbiheader );
    postMessage(offset);
    postMessage(range);
    let bgz = new BGZipFile(bgzipdatafile);
    tdr = getBGZipReader(bgz, offset.offset, offset.voffset);
    let comp = getRowRangeComparer(range, tbiheader);
    let matches = [];
    let passed = 0;
    let cols, stat;
    while (tdr.hasMore && matches.length < 1000 && !passed) {
        cols = tdr.readColumns();
        stat = comp(cols);
        if (stat === 0) {
            matches.push(cols.join(","));
        } else if (stat === 1) {
            passed = true;
        }
    }
    postMessage({box: matches.join("\n")});
}

function parseRange(rawrange) {
    let cp = rawrange.replace(/\s/g, "").split(":");
    if (cp.length === 1) {
        return [cp[0]];
    }
    let ss = cp[1].replace(/[^\d-+\xb1kmKMbB]/g, "").toLowerCase();
    let re = /(\d+)([+-\xb1])(\d+)([km]?)b?/;
    let parts = re.exec(ss);
    if (parts) {
        let start = parseInt(parts[1]);
        let diff = parts[2];
        let end = parseInt(parts[3]);
        let units = parts[4];
        if (diff === "+" || diff === "\xb1") {
            if (units === "k") {
                end = end * 1e3;
            } else if (units === "m") {
                end = end * 1e6;
            }
            start = start - end;
            end = start + 2 * end;
        }
        return [cp[0], start, end];
    } else {
        throw "Invalid range format"
    }
}

function getRowRangeComparer(range, header) {
    //0 if in range, -1 if before, 1 if after
    let colSeq = header.colSeq - 1;
    let colStart = header.colStart - 1;
    let colEnd = ((header.colEnd) ? header.colEnd : header.colStart) - 1;
    return function (row) {
        if (row[colSeq] === range[0]) {
            if (row[colEnd] < range[1]) {
                return -1
            } else if (row[colStart] > range[2]) {
                return 1
            } else {
                return 0
            }
        } else {
            let rangeSeqIndex = header.seqNames.indexOf(range[0]);
            let rowSeqIndex = header.seqNames.indexOf(row[colSeq]);
            if (rowSeqIndex < rangeSeqIndex) {
                return -1;
            } else {
                return 1;
            }
        }
    }
}

function getBGZipReader(bgz, fileoffset, virtualoffset) {
    function DataSource (f) {
        let _f = f;
        this.next = function () {
            return _f.getBlockAsArrayBuffer().buffer;
        }
    }
    let ds = new DataSource(bgz);
    var ab;
    if (fileoffset) {
        ab = bgz.getBlockAsArrayBuffer(fileoffset).buffer;
    } else {
        ab = new ArrayBuffer(0);
    }
    let dr = new DataReader(ab, true, ds);
    if (virtualoffset) {
        dr.skip(virtualoffset);
    }
    return dr;
}

function BGZipFile(file) {
    let index = 0;
    let _this = this;
    let _file = file;


    this.getGzipHeader = function (offset) {
        offset = offset || index;
        let blob = _file.slice(offset, offset + 500);
        let reader = new FileReaderSync();
        let ab = reader.readAsArrayBuffer(blob);
        let dr = new DataReader(ab, true);

        let gzh = {};
        gzh.id1 = dr.readUint8();
        gzh.id2 = dr.readUint8();
        if (gzh.id1 !== 31 || gzh.id2 !== 139) {
            return null;
        }
        gzh.cm = dr.readUint8();

        gzh.flag = {};
        gzh.flag.raw = dr.readUint8();
        gzh.flag.isText = (gzh.flag.raw & 1);
        gzh.flag.hasHcrc = (gzh.flag.raw & 2);
        gzh.flag.hasExtra = (gzh.flag.raw & 4);
        gzh.flag.hasName = (gzh.flag.raw & 8);
        gzh.flag.hasComment = (gzh.flag.raw & 16);

        gzh.time = dr.readUint32();
        gzh.xfl = dr.readUint8();
        gzh.os = dr.readUint8();

        gzh.extra = [];
        if (gzh.flag.hasExtra) {
            let extralen = dr.readUint16();
            let extrastart = dr.getIndex();
            gzh.xlen = extralen;
            while (dr.getIndex() < extrastart + extralen) {
                let ex = {};
                ex.si = dr.readString(2);
                ex.slen = dr.readUint16();
                if (ex.si === "BC" && ex.slen === 2) {
                    //is BGZIP
                    ex.content = dr.readUint16();
                    gzh.bgzip = {blockSize: ex.content};
                } else {
                    ex.content = dr.readString(slen);
                    //dr.skip(slen);
                }
                gzh.extra.push(ex);
            }
        }
        if (gzh.flag.hasName) {
            gzh.filename = dr.readNullTerminatedString();
        }
        if (gzh.flag.hasComment) {
            gzh.comment = dr.readNullTerminatedString();
        }
        if (gzh.flag.hasHcrc) {
            gzh.crc16 = dr.readUint16;
        }
        gzh.totalLength = dr.getIndex();
        return gzh;
    };

    this.getBlockAsString = function (offset) {
        index = offset || index || 0;
        if (index > _file.size) {
            return null;
        }
        let gzh = _this.getGzipHeader(index);
        let dataoffset = index + gzh.totalLength;
        let datawidth = gzh.bgzip.blockSize - gzh.xlen - 19;
        let blob = _file.slice(dataoffset, dataoffset + datawidth);
        let reader = new FileReaderSync();
        let data = RawDeflate.inflate(reader.readAsBinaryString(blob));

        index += gzh.bgzip.blockSize + 1;
        return ({header: gzh, data: data, next: index})
    };

    this.getBlockAsArrayBuffer = function (offset) {
        index = offset || index || 0;
        if (index > _file.size) {
            return null;
        }
        let gzh = _this.getGzipHeader(index);
        let dataoffset = index + gzh.totalLength;
        let datawidth = gzh.bgzip.blockSize - gzh.xlen - 19;
        let blob = _file.slice(dataoffset, dataoffset + datawidth);
        let reader = new FileReaderSync();
        let data = str2ab(RawDeflate.inflate(reader.readAsBinaryString(blob)));

        index += gzh.bgzip.blockSize + 1;
        return ({header: gzh, buffer: data, next: index})
    }

};

function str2ab(str) {
    let buf = new ArrayBuffer(str.length);
    let bufView = new Uint8Array(buf);
    for (let i = 0, strLen = str.length; i < strLen; i++) {
        bufView[i] = str.charCodeAt(i);
    }
    return buf;
}

function DataReader(ab, little, source) {
    let index = 0;
    let _this = this;
    let littleEndian = little || false;
    let dv = new DataView(ab);

    this.check = function (size) {
        if (index + size <= dv.byteLength) {
            return true;
        }
        let more;
        if (source && source.next && (more = source.next())) {
            let firstLength = dv.byteLength - index;
            let ab = new ArrayBuffer(firstLength + more.byteLength);
            let newdata = new Uint8Array(more);
            let extended = new Uint8Array(ab);
            for (let i = 0; i < firstLength; i++) {
                extended[i] = dv.getUint8(index + i);
            }
            extended.set(newdata, firstLength);
            dv = new DataView(ab);
            index = 0;
            return _this.check(size);
        } else {
            return false;
        }
    };
    this.advance = function (x) {
        index += x;
    };

    this.readUint8 = function () {
        _this.check(1);
        let val = dv.getUint8(index);
        _this.advance(1);
        return val;
    };
    this.readUint8Array = function (len) {
        let r = [];
        for (let i = 0; i < len; i++) {
            r.push(_this.readUint8());
        }
        return r;
    };
    this.readUint16 = function () {
        _this.check(2);
        let val = dv.getUint16(index, littleEndian);
        _this.advance(2);
        return val;
    };
    this.readUint32 = function () {
        _this.check(4);
        let val = dv.getUint32(index, littleEndian);
        _this.advance(4);
        return val;
    };
    this.readInt32 = function () {
        _this.check(4);
        let val = dv.getInt32(index, littleEndian);
        _this.advance(4);
        return val;
    };

    this.readString = function (len) {
        let str = "";
        for (let i = 0; i < len; i++) {
            _this.check(1);
            str += String.fromCharCode(dv.getUint8(index));
            _this.advance(1);
        }
        return str;
    };
    this.readNullTerminatedString = function () {
        let str = "";
        _this.check(1);
        let val = dv.getUint8(index);
        while (val !== 0) {
            str += String.fromCharCode(val);
            _this.advance(1);
            _this.check(1);
            val = dv.getUint8(index);
        }
        _this.advance(1);
        return str;
    };
    this.readNullSeparatedArray = function (maxlen) {
        _this.check(1);
        let start = index;
        let vals = [];
        while (index - start < maxlen) {
            vals.push(_this.readNullTerminatedString());
        }
        return vals;

    };
    this.readColumns = function () {
        let str = "";
        let vals = [];
        _this.check(1);
        let val = dv.getUint8(index);
        while (val !== 10 && val !== 13) { //cr or lf
            if (val === 9) { //tab
                vals.push(str);
                str = "";
            } else {
                str += String.fromCharCode(val);
            }
            _this.advance(1);
            _this.check(1);
            val = dv.getUint8(index);
        }
        if (str.length) {
            vals.push(str);
            str = "";
        }
        _this.advance(1);
        if (_this.check(1) && dv.getUint8(index + 1) === 10) { //check cr+lf
            _this.advance(1);
        }
        return vals;
    };
    this.hasMore = function (x) {
        _this.check(1);
        return index <= dv.byteLength - (x || 1);
    };
    this.getIndex = function () {
        return index;
    };
    this.skip = function (x) {
        _this.check(x);
        _this.advance(x)
    }
}

function readTabixDataHeader(dr) {
    let tbi = {};
    tbi.header = dr.readString(4);
    if (tbi.header !== "TBI\1") {
        return null;
    }
    tbi.nref = dr.readInt32();
    tbi.format = dr.readInt32();
    tbi.colSeq = dr.readInt32();
    tbi.colStart = dr.readInt32();
    tbi.colEnd = dr.readInt32();
    tbi.meta = dr.readString(1);
    dr.skip(3);
    tbi.skip = dr.readInt32();
    let namelength = dr.readInt32();
    tbi.seqNames = dr.readNullSeparatedArray(namelength);

    return tbi;
};

function findTabixBins (start, stop) {
    let k = 0;
    let bins = [0];
    for (k = 1 + (start >> 26); k <= 1 + (stop >> 26); ++k) bins.push(k);
    for (k = 9 + (start >> 23); k <= 9 + (stop >> 23); ++k) bins.push(k);
    for (k = 73 + (start >> 20); k <= 73 + (stop >> 20); ++k) bins.push(k);
    for (k = 585 + (start >> 17); k <= 585 + (stop >> 17); ++k) bins.push(k);
    for (k = 4681 + (start >> 14); k <= 4681 + (stop >> 14); ++k) bins.push(k);
    return (bins);
}

function findTabixOffset (dr, header, chr, pos) {
    let seqindex = header.seqNames.indexOf(chr);
    if (seqindex < 0) {
        throw "Region " + chr + " not found in tabix header";
    }
    let intindex = Math.floor(pos / 16384);
    postMessage({pos: pos, intindex: intindex});
    let bins, binno, chunks, intervals;
    for (let section = 0; section <= seqindex && dr.hasMore(); section++) {
        bins = dr.readInt32();
        for (let b = 0; b < bins; b++) {
            binno = dr.readUint32();
            chunks = dr.readInt32();
            dr.skip(16 * chunks);
        }
        intervals = dr.readInt32();
        if (section !== seqindex) {
            dr.skip(8 * intervals);
        } else {
            if (intindex > intervals - 1) {
                throw "Invalid interval, need " + indindex + ", found " + intervals;
            }
            //dr.skip(8*intindex);
            let offset, highbits, voffset;
            for (let skip = 0; skip <= intindex; skip++) {
                voffset = dr.readUint16();
                offset = dr.readUint32();
                highbits = dr.readUint16();
                postMessage({offset: offset, voffset: voffset, skip: skip});
            }
            while (voffset === 0 && offset === 0 && intindex < intervals) {
                //at start of chr, these offsets are not set till first read is found
                //so advance till first non-zero offset
                intindex++;
                voffset = dr.readUint16();
                offset = dr.readUint32();
                highbits = dr.readUint16();
            }
            if (highbits !== 0) {
                throw "Invalid virtual offset, >32bit"
            }
            return {offset: offset, voffset: voffset};
        }
    }
}

