package org.dswarm.wikidataimporter.test;

import java.net.URL;

import com.google.common.io.Resources;
import org.junit.Test;

import org.dswarm.wikidataimporter.Executer;

/**
 * @author tgaengler
 */
public class WikidataDswarmImporterTest {

	@Test
	public void wikidataDswarmImporterTest() {

		final URL resourceURL = Resources.getResource("lic_dmp_01_v1.csv.gson");

		final String[] args = new String[] {
				resourceURL.getPath()
		};

		Executer.main(args);
	}

}
