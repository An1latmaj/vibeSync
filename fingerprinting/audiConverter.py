import os
import numpy as np
import librosa
from pydub import AudioSegment
import soundfile as sf
from typing import List, Tuple


def convert_audio_to_raw_samples(file_path: str) -> Tuple[List[int], int]:
    """
    Convert any audio file to 16-bit mono 16kHz PCM samples for fingerprinting.

    Args:
        file_path: Path to the audio file to convert

    Returns:
        Tuple containing:
        - List of 16-bit integer samples
        - Original sample rate of the audio file
    """
    # Get file extension to determine processing approach
    _, ext = os.path.splitext(file_path)
    ext = ext.lower()

    # Method 1: Use librosa for most audio formats
    if ext not in ['.mp3', '.wav', '.aac', '.ogg', '.flac']:
        # For less common formats, use librosa's generic loading
        try:
            audio, original_sr = librosa.load(file_path, sr=None, mono=True)
            audio_resampled = librosa.resample(audio, orig_sr=original_sr, target_sr=16000)
            samples = (audio_resampled * 32767).astype(np.int16).tolist()
            return samples, original_sr
        except Exception as e:
            raise RuntimeError(f"Failed to process {file_path} with librosa: {e}")

    # Method 2: Use pydub for common formats (better handling of metadata)
    try:
        audio = AudioSegment.from_file(file_path)
        original_sr = audio.frame_rate

        # Convert to mono if stereo
        if audio.channels > 1:
            audio = audio.set_channels(1)

        # Resample to 16kHz
        if audio.frame_rate != 16000:
            audio = audio.set_frame_rate(16000)

        # Ensure 16-bit PCM
        if audio.sample_width != 2:
            audio = audio.set_sample_width(2)

        # Extract raw samples
        samples = np.array(audio.get_array_of_samples(), dtype=np.int16).tolist()
        return samples, original_sr

    except Exception as e:
        # Fall back to librosa if pydub fails
        try:
            audio, original_sr = librosa.load(file_path, sr=None, mono=True)
            audio_resampled = librosa.resample(audio, orig_sr=original_sr, target_sr=16000)
            samples = (audio_resampled * 32767).astype(np.int16).tolist()
            return samples, original_sr
        except Exception as e2:
            raise RuntimeError(f"Failed to process {file_path}: {e}, {e2}")


def save_as_wav_file(samples: List[int], output_path: str) -> None:
    """
    Save processed samples as a WAV file with the correct format.

    Args:
        samples: List of 16-bit integer samples
        output_path: Path to save the WAV file
    """
    samples_array = np.array(samples, dtype=np.int16)
    sf.write(output_path, samples_array, 16000, subtype='PCM_16')