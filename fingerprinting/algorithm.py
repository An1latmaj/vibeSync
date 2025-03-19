#!/usr/bin/python3
from numpy import fft, array, maximum, log, hanning, abs
from typing import Dict, List, Optional, Any, TypeVar, Generic
from enum import IntEnum
from copy import copy

# Pre-compute the Hanning matrix once (wipe trailing and leading zeros)
HANNING_MATRIX = hanning(2050)[1:-1]

from fingerprinting.signatureFormat import DecodedMessage, FrequencyPeak, FrequencyBand

T = TypeVar('T')


class RingBuffer(Generic[T]):
    """Efficient ring buffer implementation with type annotations."""

    def __init__(self, buffer_size: int, default_value: Optional[T] = None):
        self.buffer: List[T] = [copy(default_value) for _ in range(buffer_size)] if default_value is not None else [
                                                                                                                       None] * buffer_size  # type: ignore
        self.position: int = 0
        self.buffer_size: int = buffer_size
        self.num_written: int = 0

    def append(self, value: T) -> None:
        """Add a value to the ring buffer."""
        self.buffer[self.position] = value
        self.position = (self.position + 1) % self.buffer_size
        self.num_written += 1

    def __getitem__(self, index):
        if isinstance(index, slice):
            # Handle slicing
            return [self.buffer[i % self.buffer_size] for i in range(
                index.start if index.start is not None else 0,
                index.stop if index.stop is not None else self.buffer_size,
                index.step if index.step is not None else 1
            )]
        # Handle integer indexing
        return self.buffer[index % self.buffer_size]

    def __setitem__(self, index, value):
        if isinstance(index, slice):
            # Handle slice assignment
            start = index.start if index.start is not None else 0
            stop = index.stop if index.stop is not None else self.buffer_size
            step = index.step if index.step is not None else 1

            for i, v in zip(range(start, stop, step), value):
                self.buffer[i % self.buffer_size] = v
        else:
            # Handle integer indexing
            self.buffer[index % self.buffer_size] = value


class SignatureGenerator:
    def __init__(self):
        # Configuration
        self.MAX_TIME_SECONDS = 30.0
        self.MAX_PEAKS = 255
        self.SAMPLE_RATE = 16000

        # Processing state
        self.input_pending_processing: List[int] = []
        self.samples_processed: int = 0

        # Ring buffers
        self.ring_buffer_of_samples: RingBuffer[int] = RingBuffer(buffer_size=2048, default_value=0)
        self.fft_outputs: RingBuffer[List[float]] = RingBuffer(buffer_size=256, default_value=[0.0] * 1025)
        self.spread_ffts_output: RingBuffer[List[float]] = RingBuffer(buffer_size=256, default_value=[0] * 1025)

        # Initialize signature object
        self.reset_signature()

    def reset_signature(self) -> None:
        """Reset the signature object and buffers for the next processing cycle."""
        self.next_signature = DecodedMessage()
        self.next_signature.sample_rate_hz = self.SAMPLE_RATE
        self.next_signature.number_samples = 0
        self.next_signature.frequency_band_to_sound_peaks = {}

        self.ring_buffer_of_samples = RingBuffer(buffer_size=2048, default_value=0)
        self.fft_outputs = RingBuffer(buffer_size=256, default_value=[0.0] * 1025)
        self.spread_ffts_output = RingBuffer(buffer_size=256, default_value=[0] * 1025)

    def feed_input(self, s16le_mono_samples: List[int]) -> None:
        """Add signed 16-bit 16 KHz mono PCM samples for signature generation."""
        self.input_pending_processing.extend(s16le_mono_samples)

    def get_next_signature(self) -> Optional[DecodedMessage]:
        """
        Process pending input samples and return a signature when enough data is gathered.
        Returns None if no more samples to process.
        """
        if len(self.input_pending_processing) - self.samples_processed < 128:
            return None

        # Process available samples until we reach time/peak limits
        while (len(self.input_pending_processing) - self.samples_processed >= 128 and
               (self.next_signature.number_samples / self.next_signature.sample_rate_hz < self.MAX_TIME_SECONDS or
                sum(len(peaks) for peaks in
                    self.next_signature.frequency_band_to_sound_peaks.values()) < self.MAX_PEAKS)):
            chunk = self.input_pending_processing[self.samples_processed:self.samples_processed + 128]
            self.process_input(chunk)
            self.samples_processed += 128

        # Return the completed signature and reset for next one
        returned_signature = self.next_signature
        self.reset_signature()
        return returned_signature

    def process_input(self, s16le_mono_samples: List[int]) -> None:
        """Process a batch of audio samples to extract features."""
        self.next_signature.number_samples += len(s16le_mono_samples)

        for position in range(0, len(s16le_mono_samples), 128):
            chunk = s16le_mono_samples[position:position + 128]
            self.do_fft(chunk)
            self.do_peak_spreading_and_recognition()

    def do_fft(self, batch_of_128_s16le_mono_samples: List[int]) -> None:
        """Perform Fast Fourier Transform on the audio samples."""
        # Update ring buffer with new samples
        end_pos = (self.ring_buffer_of_samples.position + len(
            batch_of_128_s16le_mono_samples)) % self.ring_buffer_of_samples.buffer_size

        if end_pos > self.ring_buffer_of_samples.position:
            # Contiguous segment
            self.ring_buffer_of_samples.buffer[
            self.ring_buffer_of_samples.position:end_pos] = batch_of_128_s16le_mono_samples
        else:
            # Wrapping around the buffer
            first_part = self.ring_buffer_of_samples.buffer_size - self.ring_buffer_of_samples.position
            self.ring_buffer_of_samples.buffer[self.ring_buffer_of_samples.position:] = batch_of_128_s16le_mono_samples[
                                                                                        :first_part]
            self.ring_buffer_of_samples.buffer[:end_pos] = batch_of_128_s16le_mono_samples[first_part:]

        self.ring_buffer_of_samples.position = end_pos
        self.ring_buffer_of_samples.num_written += len(batch_of_128_s16le_mono_samples)

        # Extract data from ring buffer
        excerpt_from_ring_buffer = (
                self.ring_buffer_of_samples[self.ring_buffer_of_samples.position:] +
                self.ring_buffer_of_samples[:self.ring_buffer_of_samples.position]
        )

        # Apply Hanning window and perform FFT
        fft_results = fft.rfft(HANNING_MATRIX * excerpt_from_ring_buffer)

        # Calculate magnitude
        fft_results = (fft_results.real ** 2 + fft_results.imag ** 2) / (1 << 17)
        fft_results = maximum(fft_results, 1e-10)  # Avoid very small values

        self.fft_outputs.append(fft_results)

    def do_peak_spreading_and_recognition(self) -> None:
        """Analyze FFT outputs to detect and spread peaks."""
        self.do_peak_spreading()

        if self.spread_ffts_output.num_written >= 46:
            self.do_peak_recognition()

    def do_peak_spreading(self) -> None:
        """Perform frequency-domain and time-domain spreading of peak values."""
        origin_last_fft = self.fft_outputs[self.fft_outputs.position - 1]
        spread_last_fft = list(origin_last_fft)

        # Frequency-domain spreading
        for position in range(1023):
            spread_last_fft[position] = max(spread_last_fft[position:position + 3])

        # Time-domain spreading
        for position in range(1025):
            max_value = spread_last_fft[position]

            for former_fft_num in [-1, -3, -6]:
                idx = (self.spread_ffts_output.position + former_fft_num) % self.spread_ffts_output.buffer_size
                former_fft_output = self.spread_ffts_output[idx]
                former_fft_output[position] = max_value = max(former_fft_output[position], max_value)

        self.spread_ffts_output.append(spread_last_fft)

    def do_peak_recognition(self) -> None:
        """Identify significant frequency peaks from the spread FFT outputs."""
        fft_minus_46 = self.fft_outputs[(self.fft_outputs.position - 46) % self.fft_outputs.buffer_size]
        fft_minus_49 = self.spread_ffts_output[
            (self.spread_ffts_output.position - 49) % self.spread_ffts_output.buffer_size]
        fft_minus_53 = self.spread_ffts_output[
            (self.spread_ffts_output.position - 53) % self.spread_ffts_output.buffer_size]
        fft_minus_45 = self.spread_ffts_output[
            (self.spread_ffts_output.position - 45) % self.spread_ffts_output.buffer_size]

        # Define frequency bands
        FREQ_BANDS = {
            (250, 520): FrequencyBand._250_520,
            (520, 1450): FrequencyBand._520_1450,
            (1450, 3500): FrequencyBand._1450_3500,
            (3500, 5500): FrequencyBand._3500_5500
        }

        for bin_position in range(10, 1015):
            # Check if bin is large enough to be a peak
            if fft_minus_46[bin_position] < 1 / 64 or fft_minus_46[bin_position] < fft_minus_49[bin_position - 1]:
                continue

            # Check frequency-domain local minimum
            neighbor_offsets = [*range(-10, -3, 3), -3, 1, *range(2, 9, 3)]
            max_neighbor = max(fft_minus_49[bin_position + offset] for offset in neighbor_offsets)

            if fft_minus_46[bin_position] <= max_neighbor:
                continue

            # Check time-domain local minimum
            other_offsets = [-53, -45, *range(165, 201, 7), *range(214, 250, 7)]
            max_other_neighbor = max(
                self.spread_ffts_output[(self.spread_ffts_output.position + offset) %
                                        self.spread_ffts_output.buffer_size][bin_position - 1]
                for offset in other_offsets
            )

            if fft_minus_46[bin_position] <= max_other_neighbor:
                continue

            # This is a peak - calculate its properties
            fft_number = self.spread_ffts_output.num_written - 46

            # Calculate peak magnitude and correction
            peak_magnitude = log(max(1 / 64, fft_minus_46[bin_position])) * 1477.3 + 6144
            peak_magnitude_before = log(max(1 / 64, fft_minus_46[bin_position - 1])) * 1477.3 + 6144
            peak_magnitude_after = log(max(1 / 64, fft_minus_46[bin_position + 1])) * 1477.3 + 6144

            peak_variation_1 = peak_magnitude * 2 - peak_magnitude_before - peak_magnitude_after

            if peak_variation_1 <= 0:
                continue

            peak_variation_2 = (peak_magnitude_after - peak_magnitude_before) * 32 / peak_variation_1
            corrected_peak_frequency_bin = bin_position * 64 + peak_variation_2

            # Determine frequency band
            frequency_hz = corrected_peak_frequency_bin * (self.SAMPLE_RATE / 2 / 1024 / 64)

            if frequency_hz < 250:
                continue

            # Assign to appropriate frequency band
            band = None
            for (low, high), freq_band in FREQ_BANDS.items():
                if low <= frequency_hz < high:
                    band = freq_band
                    break

            if band is None:
                continue

            # Store the peak
            if band not in self.next_signature.frequency_band_to_sound_peaks:
                self.next_signature.frequency_band_to_sound_peaks[band] = []

            self.next_signature.frequency_band_to_sound_peaks[band].append(
                FrequencyPeak(
                    fft_number,
                    int(peak_magnitude),
                    int(corrected_peak_frequency_bin),
                    self.SAMPLE_RATE
                )
            )