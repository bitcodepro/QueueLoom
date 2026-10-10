import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('refresh_emulator_images', Path(__file__).resolve().parents[1] / 'refresh-emulator-images.py')
refresh = importlib.util.module_from_spec(spec)
spec.loader.exec_module(refresh)


class RefreshIntegrityTests(unittest.TestCase):
    def test_bad_config_digest_platform_or_rootfs_rejected_even_with_python_optimization(self):
        for fault in ('digest', 'platform', 'rootfs'):
            with self.subTest(fault=fault):
                config_id = 'sha256:'+'1'*64
                config = {'os': 'linux', 'architecture': 'amd64', 'rootfs': {'diff_ids': ['sha256:'+'2'*64]}}
                actual = config_id
                if fault == 'digest':
                    actual = 'sha256:'+'0'*64
                elif fault == 'platform':
                    config['architecture'] = 'arm64'
                else:
                    config['rootfs']['diff_ids'] = ['not-a-digest']
                with patch.object(refresh, 'Registry') as registry:
                    registry.return_value.get.side_effect = [({'config': {'digest': config_id}, 'layers': []}, 'sha256:'+'3'*64), (config, actual)]
                    with self.assertRaises(RuntimeError):
                        refresh.resolve('kafka')

    def test_check_mode_rejects_changed_registry_metadata_even_with_python_optimization(self):
        with self.assertRaisesRegex(RuntimeError, 'checked-in lock'):
            refresh.verify_checked_lock({'images': ['changed']}, {'images': ['original']})


if __name__ == '__main__':
    unittest.main()
