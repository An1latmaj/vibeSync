#!/usr/bin/python3
from typing import Dict, List, TypedDict, ClassVar, Final
from base64 import b64decode, b64encode
from math import  exp, sqrt
from binascii import crc32
from enum import IntEnum
from io import BytesIO
from ctypes import *
from dataclasses import dataclass, field

# DATA_URI_PREFIX: Final[str] = 'data:audio/vnd.shazam.sig;base64,'


class SampleRate(IntEnum):  # Enum keys are sample rates in Hz
    _8000 = 1
    _11025 = 2
    _16000 = 3
    _32000 = 4
    _44100 = 5
    _48000 = 6


class FrequencyBand(IntEnum):  # Enum keys are frequency ranges in Hz
    _0_250 = -1  # Nothing above 250 Hz is actually stored
    _250_520 = 0
    _520_1450 = 1
    _1450_3500 = 2
    _3500_5500 = 3  # 3.5 KHz - 5.5 KHz should not be used in legacy mode


class RawSignatureHeader(LittleEndianStructure):
    _pack_ = True

    _fields_ = [
        ('magic1', c_uint32),
        ('crc32', c_uint32),  # CRC-32 for all following data (excluding first 8 bytes)
        ('size_minus_header', c_uint32),  # Total size minus header (48 bytes)
        ('magic2', c_uint32),  # Fixed 0x94119c00 - 00 9c 11 94
        ('void1', c_uint32 * 3),  # Void
        ('shifted_sample_rate_id', c_uint32),  # SampleRate left-shifted by 27
        ('void2', c_uint32 * 2),  # Void, or for "rolling window" mode
        ('number_samples_plus_divided_sample_rate', c_uint32),  # samples + sample_rate * 0.24
        ('fixed_value', c_uint32)  # Calculated as ((15 << 19) + 0x40000)
    ]


class PeakEncodingConstants:
    """Constants used for frequency peak encoding/decoding."""
    FFT_PASS_MARKER: Final[int] = 0xff
    FFT_PASS_LENGTH: Final[int] = 4
    PEAK_MAGNITUDE_BYTES: Final[int] = 2
    PEAK_FREQUENCY_BYTES: Final[int] = 2
    PEAK_RECORD_LENGTH: Final[int] = 5  # 1 + 2 + 2


@dataclass
class FrequencyPeak:
    """A single frequency peak detected in the audio."""
    __slots__ = ('fft_pass_number', 'peak_magnitude', 'corrected_peak_frequency_bin', 'sample_rate_hz')

    fft_pass_number: int
    peak_magnitude: int
    corrected_peak_frequency_bin: int
    sample_rate_hz: int

    def get_frequency_hz(self) -> float:
        """Convert FFT bin to frequency in Hz."""
        return self.corrected_peak_frequency_bin * (self.sample_rate_hz / 2 / 1024 / 64)

    def get_amplitude_pcm(self) -> float:
        """Calculate amplitude in PCM."""
        return sqrt(exp((self.peak_magnitude - 6144) / 1477.3) * (1 << 17) / 2) / 1024

    def get_seconds(self) -> float:
        """Calculate time position in seconds."""
        return (self.fft_pass_number * 128) / self.sample_rate_hz


class FrequencyPeakJSON(TypedDict):
    """JSON representation of a frequency peak."""
    fft_pass_number: int
    peak_magnitude: int
    corrected_peak_frequency_bin: int
    _frequency_hz: float
    _amplitude_pcm: float
    _seconds: float


class DecodedMessageJSON(TypedDict):
    """JSON representation of a decoded message."""
    sample_rate_hz: int
    number_samples: int
    _seconds: float
    frequency_band_to_peaks: Dict[str, List[FrequencyPeakJSON]]


@dataclass
class DecodedMessage:
    """Represents a decoded audio fingerprint signature."""
    sample_rate_hz: int = 0
    number_samples: int = 0
    frequency_band_to_sound_peaks: Dict[FrequencyBand, List[FrequencyPeak]] = field(default_factory=dict)

    # Class constants
    HEADER_SIZE: ClassVar[int] = 48
    MAGIC1: ClassVar[int] = 0xcafe2580
    MAGIC2: ClassVar[int] = 0x94119c00
    TLV_TYPE_FIXED: ClassVar[int] = 0x40000000
    BAND_ID_OFFSET: ClassVar[int] = 0x60030040

    @classmethod
    def decode_from_binary(cls, data: bytes) -> 'DecodedMessage':
        """Decode a binary signature into a DecodedMessage object."""
        self = cls()
        buf = BytesIO(data)

        # Extract checksummable data
        buf.seek(8)
        checksummable_data = buf.read()
        buf.seek(0)

        # Read header
        header = RawSignatureHeader()
        buf.readinto(header)

        # Validate header
        assert header.magic1 == cls.MAGIC1, "Invalid magic1 value"
        assert header.size_minus_header == len(data) - cls.HEADER_SIZE, "Invalid size in header"
        assert crc32(checksummable_data) & 0xffffffff == header.crc32, "CRC32 checksum mismatch"
        assert header.magic2 == cls.MAGIC2, "Invalid magic2 value"

        # Extract sample rate and number of samples
        self.sample_rate_hz = int(SampleRate(header.shifted_sample_rate_id >> 27).name.strip('_'))
        self.number_samples = int(header.number_samples_plus_divided_sample_rate - self.sample_rate_hz * 0.24)

        # Skip the fixed TLV section
        assert int.from_bytes(buf.read(4), 'little') == cls.TLV_TYPE_FIXED
        assert int.from_bytes(buf.read(4), 'little') == len(data) - cls.HEADER_SIZE

        # Process frequency bands and peaks
        self._decode_frequency_peaks(buf)

        return self

    def _decode_frequency_peaks(self, buf: BytesIO) -> None:
        """Process and decode frequency peaks from the buffer."""
        while True:
            tlv_header = buf.read(8)
            if not tlv_header:
                break

            frequency_band_id = int.from_bytes(tlv_header[:4], 'little')
            frequency_peaks_size = int.from_bytes(tlv_header[4:], 'little')

            # Calculate padding and prepare buffer for frequency peaks
            frequency_peaks_padding = -frequency_peaks_size % 4
            frequency_peaks_buf = BytesIO(buf.read(frequency_peaks_size))
            buf.read(frequency_peaks_padding)  # Skip padding

            # Decode the frequency band
            frequency_band = FrequencyBand(frequency_band_id - self.BAND_ID_OFFSET)
            self.frequency_band_to_sound_peaks[frequency_band] = self._decode_band_peaks(
                frequency_peaks_buf, frequency_band
            )

    def _decode_band_peaks(self, peaks_buf: BytesIO, band: FrequencyBand) -> List[FrequencyPeak]:
        """Decode all peaks for a specific frequency band."""
        peaks: List[FrequencyPeak] = []
        fft_pass_number = 0

        while True:
            raw_fft_pass = peaks_buf.read(1)
            if not raw_fft_pass:
                break

            fft_pass_offset = raw_fft_pass[0]

            # Check for FFT pass marker
            if fft_pass_offset == PeakEncodingConstants.FFT_PASS_MARKER:
                fft_pass_number = int.from_bytes(
                    peaks_buf.read(PeakEncodingConstants.FFT_PASS_LENGTH),
                    'little'
                )
                continue

            # Update the FFT pass number with the offset
            fft_pass_number += fft_pass_offset

            # Read peak data
            peak_magnitude = int.from_bytes(
                peaks_buf.read(PeakEncodingConstants.PEAK_MAGNITUDE_BYTES),
                'little'
            )
            corrected_peak_frequency_bin = int.from_bytes(
                peaks_buf.read(PeakEncodingConstants.PEAK_FREQUENCY_BYTES),
                'little'
            )

            # Add the peak to our collection
            peaks.append(
                FrequencyPeak(
                    fft_pass_number,
                    peak_magnitude,
                    corrected_peak_frequency_bin,
                    self.sample_rate_hz
                )
            )

        return peaks

    def encode_to_json(self) -> DecodedMessageJSON:
        """Encode the current object to a JSON format for debugging."""
        return {
            "sample_rate_hz": self.sample_rate_hz,
            "number_samples": self.number_samples,
            "_seconds": self.number_samples / self.sample_rate_hz,
            "frequency_band_to_peaks": {
                frequency_band.name.strip('_'): [
                    {
                        "fft_pass_number": peak.fft_pass_number,
                        "peak_magnitude": peak.peak_magnitude,
                        "corrected_peak_frequency_bin": peak.corrected_peak_frequency_bin,
                        "_frequency_hz": peak.get_frequency_hz(),
                        "_amplitude_pcm": peak.get_amplitude_pcm(),
                        "_seconds": peak.get_seconds()
                    }
                    for peak in frequency_peaks
                ]
                for frequency_band, frequency_peaks in sorted(self.frequency_band_to_sound_peaks.items())
            }
        }

    def encode_to_binary(self) -> bytes:
        """Encode the current object to binary format."""
        # Create and initialize header
        header = RawSignatureHeader()
        header.magic1 = self.MAGIC1
        header.magic2 = self.MAGIC2
        header.shifted_sample_rate_id = int(getattr(SampleRate, f'_{self.sample_rate_hz}')) << 27
        header.fixed_value = ((15 << 19) + 0x40000)
        header.number_samples_plus_divided_sample_rate = int(self.number_samples + self.sample_rate_hz * 0.24)

        # Generate content buffer
        contents_buf = BytesIO()
        self._encode_frequency_peaks(contents_buf)
        content_bytes = contents_buf.getvalue()

        # Generate the full message
        buf = BytesIO()
        header.size_minus_header = len(content_bytes) + 8

        # Write header placeholder
        buf.write(bytes(header))

        # Write fixed TLV section
        buf.write(self.TLV_TYPE_FIXED.to_bytes(4, 'little'))
        buf.write((len(content_bytes) + 8).to_bytes(4, 'little'))

        # Write content
        buf.write(content_bytes)

        # Calculate and update CRC32
        buf.seek(8)
        header.crc32 = crc32(buf.read()) & 0xffffffff
        buf.seek(0)
        buf.write(bytes(header))

        return buf.getvalue()

    def _encode_frequency_peaks(self, contents_buf: BytesIO) -> None:
        """Encode all frequency peaks to the content buffer."""
        for frequency_band, frequency_peaks in sorted(self.frequency_band_to_sound_peaks.items()):
            if not frequency_peaks:
                continue

            peaks_buf = BytesIO()
            fft_pass_number = 0

            for peak in frequency_peaks:
                # Check if we need to insert a marker for a big jump in FFT pass numbers
                if peak.fft_pass_number - fft_pass_number >= PeakEncodingConstants.FFT_PASS_MARKER:
                    peaks_buf.write(bytes([PeakEncodingConstants.FFT_PASS_MARKER]))
                    peaks_buf.write(peak.fft_pass_number.to_bytes(
                        PeakEncodingConstants.FFT_PASS_LENGTH, 'little'
                    ))
                    fft_pass_number = peak.fft_pass_number

                # Write the peak data
                peaks_buf.write(bytes([peak.fft_pass_number - fft_pass_number]))
                peaks_buf.write(peak.peak_magnitude.to_bytes(
                    PeakEncodingConstants.PEAK_MAGNITUDE_BYTES, 'little'
                ))
                peaks_buf.write(peak.corrected_peak_frequency_bin.to_bytes(
                    PeakEncodingConstants.PEAK_FREQUENCY_BYTES, 'little'
                ))

                fft_pass_number = peak.fft_pass_number

            # Write the TLV header and data for this frequency band
            peaks_data = peaks_buf.getvalue()
            contents_buf.write((self.BAND_ID_OFFSET + int(frequency_band)).to_bytes(4, 'little'))
            contents_buf.write(len(peaks_data).to_bytes(4, 'little'))
            contents_buf.write(peaks_data)

            # Add padding to align to 4-byte boundary
            padding_bytes = -len(peaks_data) % 4
            if padding_bytes:
                contents_buf.write(b'\x00' * padding_bytes)

