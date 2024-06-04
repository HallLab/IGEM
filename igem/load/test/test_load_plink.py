from os.path import dirname, join, realpath

from load.plink import plink1_xa
from numpy import dtype
from numpy.testing import assert_array_equal, assert_equal


def test_load_plink1_xa():

    datafiles = join(dirname(realpath(__file__)), "data_files")
    # file_prefix = join(datafiles, "data")
    file_prefix = join(datafiles, "LURIC_AFFY_FINAL_clean")
    bim = file_prefix + ".bim"
    bed = file_prefix + ".bed"
    fam = file_prefix + ".fam"

    G = plink1_xa(bed, bim, fam, verbose=False)
    print(G.values)
    assert_equal(G.data.dtype, dtype("float32"))

    snp = G.where((G.chrom == "1") & (G.pos == 711153), drop=True)["snp"].values # noqa E501
    assert_array_equal(snp, ["rs12565286"])
    # G.head()

    # shape = G.where(G.chrom == "1", drop=True).shape
    # assert_array_equal(shape, [3, 10])

    # shape = G.where(G.chrom == "2", drop=True).shape
    # assert_array_equal(shape, [3, 0])

    # g = G.where((G.fid == "Sample_2") & (G.iid == "Sample_2"), drop=True)
    # assert_array_equal(g["trait"].values, ["-9"])

    # arr = [
    #     [2.0, 2.0, nan, nan, 2.0, 2.0, 2.0, 2.0, 1.0, 2.0],
    #     [2.0, 1.0, nan, nan, 2.0, 2.0, 1.0, 2.0, 2.0, 1.0],
    #     [1.0, 2.0, nan, 1.0, 2.0, 2.0, 0.0, 2.0, 2.0, 2.0],
    # ]
    # assert_array_equal(G, arr)
